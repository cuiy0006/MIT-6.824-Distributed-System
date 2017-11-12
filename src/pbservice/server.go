package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view     viewservice.View
	keyValue map[string]string
	lastArgs map[int64]PAGArgs
}

func (pb *PBServer) Get(args *PAGArgs, reply *CommonReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// The server isn't the active primary
	if pb.me != pb.view.Primary {
		reply.Err = "I am not primary"
		return nil
	}
	//success! get key, value from dictionary
	reply.Value = pb.keyValue[args.Key]

	return nil
}

func (pb *PBServer) PutAppend(args *PAGArgs, reply *CommonReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// The server isn't the active primary
	if pb.me != pb.view.Primary {
		reply.Err = "I am not primary"
		return nil
	}
	// If the operation is the last operation, then cancel it
	if *args == pb.lastArgs[args.OpId] {
		reply.Err = ""
		return nil
	}
	// record this operation Id and args
	pb.lastArgs[args.OpId] = *args

	//primary finished, let primary start
	backupReply := CommonReply{}
	for pb.view.Backup != "" {
		backupReply.Err = ""
		ok := call(pb.view.Backup, "PBServer.PutAppendBackup", args, &backupReply)
		if ok && backupReply.Err == "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	//success! put key, value in dictionary
	if args.Operation == "Put" {
		pb.keyValue[args.Key] = args.Value
	} else if args.Operation == "Append" {
		pb.keyValue[args.Key] += args.Value
	} else {
		return errors.New("Only Put or Append is accepted")
	}
	return nil
}

//
//put or append
//
func (pb *PBServer) PutAppendBackup(args *PAGArgs, reply *CommonReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//return if this is not a backup now
	if pb.me != pb.view.Backup {
		reply.Err = "I am not backup any more"
		return nil
	}

	//return if this operation is the last operation
	if *args == pb.lastArgs[args.OpId] {
		reply.Err = ""
		return nil
	}
	// record this operation Id and args
	pb.lastArgs[args.OpId] = *args

	//success! put key, value in dictionary
	if args.Operation == "Put" {
		pb.keyValue[args.Key] = args.Value
	}
	if args.Operation == "Append" {
		pb.keyValue[args.Key] += args.Value
	}

	return nil
}

//
// executed in backup server, update keyValue and lastArgs
//
func (pb *PBServer) ReproduceBackup(args *InitBackupArgs, reply *CommonReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.keyValue = args.KeyValue
	pb.lastArgs = args.LastArgs

	return nil
}

//
// ping the viewserver to acknowlege alive.
// if view changed:
//   update my view
//   if I am primary, update backup's keyValue, lastArgs
//
func (pb *PBServer) tick() {

	// Your code here.
	view, _ := pb.vs.Ping(pb.view.Viewnum)
	oldview := pb.view
	pb.view = view

	// if view changed, do some operations
	if oldview.Viewnum != view.Viewnum {
		// if current server is primary and backup changed, initialize it
		pb.mu.Lock()

		args := &InitBackupArgs{
			KeyValue: pb.keyValue,
			LastArgs: pb.lastArgs,
		}
		reply := CommonReply{}
		//update backup's lastArgs and keyValue storage
		for pb.me == view.Primary && view.Backup != oldview.Backup && view.Backup != "" {
			ok := call(pb.view.Backup, "PBServer.ReproduceBackup", args, &reply)
			if ok && reply.Err == "" {
				break
			}
		}
		pb.mu.Unlock()
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.keyValue = make(map[string]string)
	pb.lastArgs = make(map[int64]PAGArgs)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
