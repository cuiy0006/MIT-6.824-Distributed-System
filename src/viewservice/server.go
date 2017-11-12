package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	//my declare
	view         View
	pingDic      map[string]time.Time
	acked        bool
	secondBackup string
}

//
// server Ping RPC handler.
//

func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	client := args.Me
	viewNum := args.Viewnum

	vs.pingDic[client] = time.Now()

	if client == vs.view.Primary {
		if viewNum == 0 {
			vs.swap(vs.view.Backup, vs.secondBackup, client)
			//vs.swap(vs.view.Backup, client, vs.secondBackup) // this also works
		} else if vs.view.Viewnum == viewNum {
			vs.acked = true
		}
	} else if client == vs.view.Backup {
		if viewNum == 0 && vs.acked {
			vs.swap(vs.view.Primary, vs.secondBackup, client)
		}
	} else {
		if vs.view.Viewnum == 0 {
			vs.swap(client, "", "")
		} else {
			vs.secondBackup = client
		}
	}
	reply.View = vs.view
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.view
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//

func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	maxDelay := DeadPings * PingInterval

	if vs.acked && time.Now().Sub(vs.pingDic[vs.view.Backup]) >= maxDelay {
		vs.swap(vs.view.Primary, vs.secondBackup, "")
	}
	if vs.acked && time.Now().Sub(vs.pingDic[vs.view.Primary]) >= maxDelay {
		vs.swap(vs.view.Backup, vs.secondBackup, "")
	}

	if time.Now().Sub(vs.pingDic[vs.secondBackup]) >= maxDelay {
		vs.secondBackup = ""
	}

}

func (vs *ViewServer) swap(primary string, backup string, secondBackup string) {
	vs.view.Viewnum++
	vs.view.Primary = primary
	vs.view.Backup = backup
	vs.secondBackup = secondBackup
	vs.acked = false
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View{Primary: "", Backup: "", Viewnum: 0}
	vs.pingDic = make(map[string]time.Time)
	vs.acked = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
