package kvpaxos

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrOpExist = "ErrOpExist"
)

const (
	PUT    = "Put"
	APPEND = "Append"
	GET    = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpId int64
}

type GetReply struct {
	Err   Err
	Value string
}
