package shardkv

import "net"
import "fmt"
import "net/rpc"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "log"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const (
	Get             = "Get"
	Put             = "Put"
	Append          = "Append"
	Reconfiguration = "Reconfiguration"
)

type Op struct {
	CID     string // client unique ID
	Seq     int    // sequence NO
	Op      string // operation name
	Key     string
	Value   string
	History History // history the server has
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // this server's group ID

	// Your definitions here.
	currSeq      int                // this server's current sequence NO
	config       shardmaster.Config // this server's config
	keyValue     map[string]string
	seqHistory   map[string]int         // the client(string)'s lastest sequence NO(int)
	replyHistory map[string]CommonReply // the client(string)'s lastest reply(string)
}

//only return when sequence NO is decided by all servers
func (kv *ShardKV) waitForDecided(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			return v.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

//do local storage
func (kv *ShardKV) doLocalStore(v Op) {
	if v.Op == Reconfiguration {
		kv.config = kv.sm.Query(v.Seq)
		extra := v.History
		kv.combineHistory(&extra, nil)
	} else if v.Op == Put || v.Op == Append || v.Op == Get {
		var reply CommonReply
		if kv.gid != kv.config.Shards[key2shard(v.Key)] {
			reply.Err = ErrWrongGroup
		} else if v.Op == Get {
			value, ok := kv.keyValue[v.Key]
			if ok {
				reply.Err, reply.Value = OK, value
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			if v.Op == Put {
				kv.keyValue[v.Key] = v.Value
			} else if v.Op == Append {
				kv.keyValue[v.Key] += v.Value
			}
			reply.Err = OK
		}

		if reply.Err != ErrWrongGroup {
			kv.seqHistory[v.CID] = v.Seq
			kv.replyHistory[v.CID] = reply
		}
	}
}

func (kv *ShardKV) doOperation(v Op) error {
	var remoteValue Op
	for {
		// for current seq NO I have, is it pending or decided?
		status, val := kv.px.Status(kv.currSeq)

		//if decided, save it locally, else, start prepare/accept this seq with my v
		if status == paxos.Decided {
			remoteValue = val.(Op)
		} else {
			kv.px.Start(kv.currSeq, v)
			remoteValue = kv.waitForDecided(kv.currSeq)
		}

		//store all information locally, including remove/leave/move
		kv.doLocalStore(remoteValue)
		kv.px.Done(kv.currSeq)
		kv.currSeq++

		if v.Op == remoteValue.Op && v.CID == remoteValue.CID && v.Seq == remoteValue.Seq {
			break
		}
	}

	return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Seq == kv.seqHistory[args.Me] {
		reply.Value = kv.replyHistory[args.Me].Value
		reply.Err = kv.replyHistory[args.Me].Err
	} else if args.Seq > kv.seqHistory[args.Me] {
		getOp := Op{CID: args.Me, Seq: args.Seq, Op: Get, Key: args.Key}
		kv.doOperation(getOp)
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Seq == kv.seqHistory[args.Me] {
		reply.Err = kv.replyHistory[args.Me].Err
	} else if args.Seq > kv.seqHistory[args.Me] {
		putAppendOp := Op{CID: args.Me, Seq: args.Seq, Op: args.Op, Key: args.Key, Value: args.Value}
		kv.doOperation(putAppendOp)
	}

	return nil
}

type History struct {
	SeqHistory   map[string]int
	ReplyHistory map[string]CommonReply
	KeyValue     map[string]string
}

//combine history to me or to this server(kv)
func (kv *ShardKV) combineHistory(other *History, me *History) {
	for key, value := range other.KeyValue {
		if me == nil {
			kv.keyValue[key] = value
		} else {
			me.KeyValue[key] = value
		}
	}

	for client, seq := range other.SeqHistory {
		var lastSeq int
		if me == nil {
			lastSeq = kv.seqHistory[client]
		} else {
			lastSeq = me.SeqHistory[client]
		}
		if lastSeq < seq {
			if me == nil {
				kv.seqHistory[client] = seq
				kv.replyHistory[client] = other.ReplyHistory[client]
			} else {
				me.SeqHistory[client] = seq
				me.ReplyHistory[client] = other.ReplyHistory[client]
			}

		}
	}
}

//copy this server(kv)'s history to new
func (kv *ShardKV) copyHistory(shard int, new *History) {
	for client, seq := range kv.seqHistory {
		new.SeqHistory[client] = seq
		new.ReplyHistory[client] = kv.replyHistory[client]
	}
	for key, value := range kv.keyValue {
		if key2shard(key) == shard {
			new.KeyValue[key] = value
		}
	}
}

type HistoryArgs struct {
	Num   int
	Shard int
}

type HistoryReply struct {
	History History
	Err     Err
}

//send RPC request for history to every servers in a group
func (kv *ShardKV) sendHistoryRequest(gid int64, shard int) HistoryReply {
	var reply HistoryReply
	servers := kv.config.Groups[gid]
	for _, server := range servers {
		args := HistoryArgs{Num: kv.config.Num, Shard: shard}
		ok := call(server, "ShardKV.ReceiveHistoryRequest", &args, &reply)
		if ok && reply.Err == OK {
			return reply
		}
	}
	return reply
}

//receive HistoryRequest and give history of this server
func (kv *ShardKV) ReceiveHistoryRequest(args *HistoryArgs, reply *HistoryReply) error {
	//configuration number is normal
	if kv.config.Num >= args.Num {
		kv.mu.Lock()
		reply.History = History{KeyValue: make(map[string]string), SeqHistory: make(map[string]int), ReplyHistory: make(map[string]CommonReply)}
		new := reply.History
		kv.copyHistory(args.Shard, &new)
		reply.Err = OK
		kv.mu.Unlock()
	}
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//query the lastest configuration (from shardmaster)
	lastestConfig := kv.sm.Query(-1)
Loop:
	//for every unseen configuration, do reconfiguration
	for n := kv.config.Num + 1; n <= lastestConfig.Num; n++ {
		config := kv.sm.Query(n)
		//prepare a history to be used in reconfiguration Operation(will be recorded in paxos log)
		history := History{KeyValue: make(map[string]string), SeqHistory: make(map[string]int), ReplyHistory: make(map[string]CommonReply)}
		//for every shard(in this server's group). request history from its group's servers
		for shard := 0; shard < shardmaster.NShards; shard++ {
			oldGid := kv.config.Shards[shard]
			newGid := config.Shards[shard]
			myGid := kv.gid

			//unassigned group
			if oldGid == 0 {
				continue
			}
			//not my group
			if newGid != myGid {
				continue
			}
			//group not changed
			if newGid == oldGid {
				continue
			}
			reply := kv.sendHistoryRequest(oldGid, shard)
			if reply.Err != OK {
				break Loop
			}
			oldHistory := reply.History
			kv.combineHistory(&oldHistory, &history)

		}
		reconfigOp := Op{Seq: config.Num, Op: Reconfiguration, History: history}
		kv.doOperation(reconfigOp)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.keyValue = make(map[string]string)
	kv.seqHistory = make(map[string]int)
	kv.replyHistory = make(map[string]CommonReply)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
