package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"math/rand"
	"strconv"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type instance struct {
	state Fate        // instance state
	n_p   string      // proposal num
	n_a   string      // accept num
	v_a   interface{} // accept value
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	dones     []int             //index : peerId, content : seq
	instances map[int]*instance //key : seq, value : instance
	majority  int
}

const (
	OK     = "OK"
	Reject = "Reject"
)

type PrepareArgs struct {
	Seq  int
	PNum string
}

type PrepareReply struct {
	Response    string
	AcceptPnum  string
	AcceptValue interface{}
}

type AcceptArgs struct {
	Seq   int
	PNum  string
	Value interface{}
}

type AcceptReply struct {
	Response string
}

type DecideArgs struct {
	Seq   int
	Value interface{}
	PNum  string
	Me    int
	Done  int
}

type DecideReply struct {
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) getInstance(seq int) *instance {
	if _, exist := px.instances[seq]; !exist {
		px.instances[seq] = &instance{n_a: "", n_p: "", v_a: nil, state: Pending}
	}
	return px.instances[seq]
}

// send prepare
func (px *Paxos) sendPrepare(seq int, pnum string, v interface{}) (bool, interface{}) {
	arg := PrepareArgs{Seq: seq, PNum: pnum}
	cnt := 0  //how many peers respond OK
	n_a := "" //accepted pnum
	v_a := v  //accepted value

	//send prepare(n) to all servers including itself
	for i, peer := range px.peers {
		reply := PrepareReply{AcceptValue: "", AcceptPnum: "", Response: Reject}
		if i == px.me {
			px.ReceivePrepare(&arg, &reply)
		} else {
			call(peer, "Paxos.ReceivePrepare", &arg, &reply)
		}

		// v' = v_a with highest n_a
		if reply.Response == OK {
			cnt++
			if reply.AcceptPnum > n_a {
				n_a = reply.AcceptPnum
				v_a = reply.AcceptValue
			}
		}
	}

	return cnt >= px.majority, v_a
}

// receive prepare
func (px *Paxos) ReceivePrepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	inst := px.getInstance(args.Seq)
	//inst.n_p : minProposal, args.PNum : n
	//If n > minProposal then minProposal = n, return (accepted proposal, accepted value)
	//else reject
	if args.PNum > inst.n_p {
		reply.Response = OK
		//minProposal = n
		px.instances[args.Seq].n_p = args.PNum
		//return (accepted proposal, accepted value)
		reply.AcceptPnum = px.instances[args.Seq].n_a
		reply.AcceptValue = px.instances[args.Seq].v_a
	} else {
		reply.Response = Reject
	}

	return nil
}

//send accept
func (px *Paxos) sendAccept(seq int, pnum string, v interface{}) bool {
	arg := AcceptArgs{Seq: seq, PNum: pnum, Value: v}
	cnt := 0

	// send accept(n, v') to all
	for i, peer := range px.peers {
		reply := AcceptReply{Response: Reject}
		if i == px.me {
			px.ReceiveAccept(&arg, &reply)
		} else {
			call(peer, "Paxos.ReceiveAccept", &arg, &reply)
		}

		if reply.Response == OK {
			cnt++
		}
	}

	return cnt >= px.majority
}

//receive accept
func (px *Paxos) ReceiveAccept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	//args.PNum : n, inst.n_p : minProposal
	//if n >= minProposal, then acceptedProposal = minProposal = n, acceptedValue = value
	inst := px.getInstance(args.Seq)
	if args.PNum >= inst.n_p {
		reply.Response = OK
		inst.n_a = args.PNum
		inst.n_p = args.PNum
		inst.v_a = args.Value
	} else {
		reply.Response = Reject
	}

	return nil
}

//send decide
func (px *Paxos) sendDecide(seq int, pnum string, v interface{}) {
	arg := DecideArgs{Seq: seq, PNum: pnum, Value: v, Me: px.me, Done: px.dones[px.me]}
	for i, peer := range px.peers {
		reply := DecideReply{}
		if i == px.me {
			px.ReceiveDecide(&arg, &reply)
		} else {
			call(peer, "Paxos.ReceiveDecide", &arg, &reply)
		}
	}
}

//reveive decide
func (px *Paxos) ReceiveDecide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	//change local variables
	inst := px.getInstance(args.Seq)
	inst.n_p = args.PNum
	inst.n_a = args.PNum
	inst.v_a = args.Value
	inst.state = Decided
	px.dones[args.Me] = args.Done
	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	go func() {
		// if seq < min,just return
		if seq < px.Min() {
			return
		}
		for {
			pnum := strconv.FormatInt(time.Now().UnixNano(), 10) + "#" + strconv.Itoa(px.me)
			if ok, value := px.sendPrepare(seq, pnum, v); ok {
				if ok = px.sendAccept(seq, pnum, value); ok {
					px.sendDecide(seq, pnum, value)
					break
				}
			}

			if state, _ := px.Status(seq); state == Decided {
				break
			}
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq > px.dones[px.me] {
		px.dones[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	tmp := -1
	for seq := range px.instances {
		if seq > tmp {
			tmp = seq
		}
	}

	return tmp
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	tmp := px.dones[px.me]
	for _, seq := range px.dones {
		if seq < tmp {
			tmp = seq
		}
	}

	for seq, instance := range px.instances {
		if seq <= tmp && instance.state == Decided {
			delete(px.instances, seq)
		}
	}

	return tmp + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	min := px.Min()

	px.mu.Lock()
	defer px.mu.Unlock()

	if seq < min {
		return Forgotten, nil
	}

	if instance, ok := px.instances[seq]; ok {
		return instance.state, instance.v_a
	} else {
		return Pending, nil
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*instance)
	px.dones = make([]int, len(px.peers))
	for i := range px.peers {
		px.dones[i] = -1
	}
	px.majority = len(peers)/2 + 1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
