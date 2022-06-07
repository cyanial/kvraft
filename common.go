package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type GetArgs struct {
	Key         string
	ClientId    int64
	SequenceNum int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Put or Append
type PutAppendArgs struct {
	Key         string
	Value       string
	Op          string // "Put" or "Append"
	ClientId    int64
	SequenceNum int64
}

type PutAppendReply struct {
	Err Err
}
