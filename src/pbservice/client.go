package pbservice

import (
	"crypto/rand"
	"math/big"
	"time"
	"viewservice"
)

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	primary string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	return ck
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	// still don't know who is the primary
	for ck.primary == "" {
		ck.UpdatePrimary()
		time.Sleep(viewservice.PingInterval)
	}

	args := &PAGArgs{Key: key, Operation: "Get", OpId: nrand()}
	reply := CommonReply{}

	ok := call(ck.primary, "PBServer.Get", args, &reply)

	for !ok || reply.Err != "" {
		time.Sleep(viewservice.PingInterval)
		reply.Err = ""
		ck.UpdatePrimary()
		ok = call(ck.primary, "PBServer.Get", args, &reply)
	}

	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	// still don't know who is the primary
	for ck.primary == "" {
		ck.UpdatePrimary()
		time.Sleep(viewservice.PingInterval)
	}

	args := &PAGArgs{Key: key, Value: value, Operation: op, OpId: nrand()}
	reply := CommonReply{}

	ok := call(ck.primary, "PBServer.PutAppend", args, &reply)

	for !ok || reply.Err != "" {
		time.Sleep(viewservice.PingInterval)
		reply.Err = ""
		ck.UpdatePrimary()
		ok = call(ck.primary, "PBServer.PutAppend", args, &reply)
	}

}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// used for clerk to update primary
func (ck *Clerk) UpdatePrimary() {
	ck.primary = ck.vs.Primary()
}
