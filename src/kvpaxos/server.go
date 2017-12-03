package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	OpId  int64
	Key   string
	Value string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	keyValue map[string]string
	history  map[int64]bool
	currSeq  int
}

//only return when sequence NO is decided by all servers
func (kv *KVPaxos) waitForDecided(seq int) Op {
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

// local variable changes
func (kv *KVPaxos) doLocalStore(v Op) {
	//put/append value in local data storage
	if v.Op == PUT {
		kv.keyValue[v.Key] = v.Value
	} else if v.Op == APPEND {
		if curr, exist := kv.keyValue[v.Key]; exist {
			kv.keyValue[v.Key] = curr + v.Value
		} else {
			kv.keyValue[v.Key] = v.Value
		}
	}

	// record operation ID
	kv.history[v.OpId] = true
}

func (kv *KVPaxos) doOperation(v Op) {
	var remoteValue Op
	for {

		// for current sequence NO I have, is it pending or decided?
		status, val := kv.px.Status(kv.currSeq)

		// if decided, save it locally, else, start prepare/accept this seq with my v
		if status == paxos.Decided {
			remoteValue = val.(Op)
		} else {
			kv.px.Start(kv.currSeq, v)
			remoteValue = kv.waitForDecided(kv.currSeq)
		}

		// store the decided v by all servers in local keyValue map, record its operation ID
		kv.doLocalStore(remoteValue)
		// this sequence number is done for me
		kv.px.Done(kv.currSeq)
		// new sequence number
		kv.currSeq++

		// if the decided value is not my v, use new sequence NO and try my v again
		if v.OpId == remoteValue.OpId {
			break
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//keep at most once
	if _, exist := kv.history[args.OpId]; exist {
		reply.Err = ErrOpExist
	} else {
		GetOp := Op{Op: GET, Key: args.Key, OpId: args.OpId}
		kv.doOperation(GetOp)
	}

	//anyway, return a value
	if val, exist := kv.keyValue[args.Key]; exist {
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}

	if reply.Err == "" {
		reply.Err = OK
	}

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//keep at most once
	if _, exist := kv.history[args.OpId]; exist {
		reply.Err = ErrOpExist
	} else {
		appendOp := Op{Op: args.Op, OpId: args.OpId, Key: args.Key, Value: args.Value}
		kv.doOperation(appendOp)
		reply.Err = OK
	}
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.keyValue = make(map[string]string)
	kv.history = make(map[int64]bool)
	kv.currSeq = 1

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
