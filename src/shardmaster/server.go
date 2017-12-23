package shardmaster

import (
	crand "crypto/rand"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	currSeq int
}

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct {
	Op   string
	OpId int64
	Args interface{}
}

func nrand() int64 {
	max := big.NewInt(int64(int64(1) << 62))
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

//only return when sequence NO is decided by all servers
func (sm *ShardMaster) waitForDecided(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, v := sm.px.Status(seq)
		if status == paxos.Decided {
			return v.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) balance(config *Config) {
	//first get average(floor) and average(ceil)
	//i.e. NShards = 10, total 3 groups, floor = 3, ceil = 4
	lengthOfGroups := len(config.Groups)
	floor := NShards / lengthOfGroups
	ceil := floor
	rest := NShards % lengthOfGroups
	if rest != 0 {
		ceil++
	}

	//floorQueue store gid who holds shards count < floor
	//ceilQueue store gid who holds shards count == floor
	//i.e. NShards = 10, total 3 groups, floorQueue takes gid with shards count 1,2,
	//ceilQueue takes gid with shards count 3
	floorQueue := make(chan int64, lengthOfGroups)
	ceilQueue := make(chan int64, lengthOfGroups)
	queue := floorQueue

	// map gid to how many shards it has
	gidToShardCnt := make(map[int64]int)
	for i := 0; i < NShards; i++ {
		gidToShardCnt[config.Shards[i]]++
	}

	//feed floorQueue and ceilQueue
	for gid := range config.Groups {
		if gidToShardCnt[gid] < floor {
			floorQueue <- gid
		} else if gidToShardCnt[gid] == floor {
			ceilQueue <- gid
		}
	}

	for i := 0; i < NShards; i++ {
		//for this shard, if gid == 0 or gid's shard overload i.e. > 4
		if config.Shards[i] == 0 || gidToShardCnt[config.Shards[i]] > ceil {
			//if all gid from floorQueue(i.e. 1,2) have been consumed, change to ceilQueue(i.e. 3)
			if len(floorQueue) == 0 {
				queue = ceilQueue
			}
			//get a gid with 1,2 or 3 shards count to eat this shard
			belowAverageGID := <-queue
			gidToShardCnt[config.Shards[i]]--
			gidToShardCnt[belowAverageGID]++
			config.Shards[i] = belowAverageGID

			//if the gid's shards count is still < floor, then push it back to floorQueue
			//else if the gid's shards count equals to floor, then push it to ceilQueue
			if gidToShardCnt[belowAverageGID] < floor {
				floorQueue <- belowAverageGID
			} else if gidToShardCnt[belowAverageGID] == floor {
				ceilQueue <- belowAverageGID
			}
		}
	}

	//if the floorQueue still has gid in it, then select the gid with shards count == ceil to feed it
	if len(floorQueue) != 0 {
		for i := 0; i < NShards; i++ {
			//get a gid with 1,2 shards count to eat this shard(it should be from a gid with 4 shards)
			if gidToShardCnt[config.Shards[i]] == ceil {
				belowAverageGID := <-queue

				gidToShardCnt[config.Shards[i]]--
				gidToShardCnt[belowAverageGID]++
				config.Shards[i] = belowAverageGID
				if len(floorQueue) == 0 {
					break
				}
			}
		}
	}

}

func (sm *ShardMaster) doLocalStore(v Op) {
	oldConfig := sm.configs[sm.currSeq-1]

	//create new configuration, deep copy info from the old, increase Num up to "Query" or other Op
	//Just like viewservice, any Join, Leave or Move leads to a config change
	var newNum int
	if v.Op == Query {
		newNum = oldConfig.Num
	} else {
		newNum = oldConfig.Num + 1
	}
	newConfig := Config{Num: newNum, Groups: make(map[int64][]string)}
	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = v
	}
	for i, Shard := range oldConfig.Shards {
		newConfig.Shards[i] = Shard
	}

	//do operations on new configurations' Groups, and balance the distribution.
	if v.Op == Join {
		joinArgs := v.Args.(JoinArgs)
		newConfig.Groups[joinArgs.GID] = joinArgs.Servers
		sm.balance(&newConfig)

	} else if v.Op == Leave {
		leaveArgs := v.Args.(LeaveArgs)
		delete(newConfig.Groups, leaveArgs.GID)
		for i := 0; i < NShards; i++ {
			if newConfig.Shards[i] == leaveArgs.GID {
				newConfig.Shards[i] = 0
			}
		}
		sm.balance(&newConfig)

	} else if v.Op == Move {
		moveArgs := v.Args.(MoveArgs)
		newConfig.Shards[moveArgs.Shard] = moveArgs.GID

	}

	//new configuration generated
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) doOperation(v Op) error {
	var remoteValue Op
	for {
		// for current seq NO I have, is it pending or decided?
		status, val := sm.px.Status(sm.currSeq)

		//if decided, save it locally, else, start prepare/accept this seq with my v
		if status == paxos.Decided {
			remoteValue = val.(Op)
		} else {
			sm.px.Start(sm.currSeq, v)
			remoteValue = sm.waitForDecided(sm.currSeq)
		}

		//store all information locally, including remove/leave/move
		sm.doLocalStore(remoteValue)
		sm.px.Done(sm.currSeq)
		sm.currSeq++

		//if operation id == remote value's opid, it means I win the paxos, operation succeeded
		if v.OpId == remoteValue.OpId {
			break
		}
	}

	return nil
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Args: *args, Op: Join, OpId: nrand()}
	sm.doOperation(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Args: *args, Op: Leave, OpId: nrand()}
	sm.doOperation(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Args: *args, Op: Move, OpId: nrand()}
	sm.doOperation(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Args: *args, Op: Query, OpId: nrand()}
	sm.doOperation(op)

	//if num == -1, the client ask for the latest configuration
	//else it ask the corresponding NO's configuration
	if args.Num == -1 {
		reply.Config = sm.configs[sm.currSeq-1]
	} else {
		for _, config := range sm.configs {
			if config.Num == args.Num {
				reply.Config = config
			}
		}
	}

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = make(map[int64][]string)
	sm.currSeq = 1

	rpcs := rpc.NewServer()

	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
