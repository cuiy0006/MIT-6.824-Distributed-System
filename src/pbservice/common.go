package pbservice

import (
	"fmt"
	"net/rpc"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// // Put or Append or Get
type PAGArgs struct {
	Key       string
	Value     string
	Operation string
	OpId      int64
}

// type PutAppendReply struct {
// 	Err Err
// }

// type GetReply struct {
// 	Err   Err
// 	Value string
// }

type CommonReply struct {
	Err   Err
	Value string
}

type InitBackupArgs struct {
	KeyValue map[string]string
	LastArgs map[int64]PAGArgs
}

type InitBackupReply struct {
	Err Err
}

type ForwardRequestReply struct {
	Err Err
}

// Your RPC definitions here.
//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
