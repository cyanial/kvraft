package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyanial/raft/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	mu          sync.Mutex
	clientId    int64
	sequenceNum int64
	leaderId    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {

	ck := &Clerk{}
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.servers = servers
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.leaderId = 0

	DPrintf("Init Clerk: %d", ck.clientId)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	args := &GetArgs{
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
	}
	reply := &GetReply{}

	leaderId := ck.currentLeader()

	for {

		DPrintf("[Client %d, leader=%d] Get, k=%s - ", ck.clientId, leaderId, key)

		if ck.servers[leaderId].Call("KVServer.Get", args, reply) {
			if reply.Err == OK {
				DPrintf("[Client %d, leader=%d] Get, k=%s, v=%s - OK", ck.clientId, leaderId, key, reply.Value)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				DPrintf("[Client %d, leader=%d] Get, k=%s - No Key", ck.clientId, leaderId, key)
				return ""
			} else if reply.Err == ErrWrongLeader {
				DPrintf("[Client %d, leader=%d] Get, k=%s - Wrong Leader", ck.clientId, leaderId, key)
				leaderId = ck.changeLeader()
			} else {
				DPrintf("[Client %d, leader=%d] Get, k=%s - other: %v", ck.clientId, leaderId, key, reply.Err)
			}
		} else {
			DPrintf("call KVServer.Get failed")
		}
		time.Sleep(1 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	args := &PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
	}
	reply := &PutAppendReply{}

	leaderId := ck.currentLeader()

	for {

		DPrintf("[Client %d, leader=%d] PutAppend, k=%s, v=%s, op=%s - ",
			ck.clientId, leaderId, key, value, op)

		if ck.servers[leaderId].Call("KVServer.PutAppend", args, reply) {
			if reply.Err == OK {
				DPrintf("[Client %d, leader=%d] PutAppend, k=%s, v=%s, op=%s - OK ",
					ck.clientId, leaderId, key, value, op)
				return
			} else if reply.Err == ErrWrongLeader {
				DPrintf("[Client %d, leader=%d] PutAppend, k=%s, v=%s, op=%s - Wrong Leader",
					ck.clientId, leaderId, key, value, op)
				leaderId = ck.changeLeader()
			} else {
				DPrintf("[Client %d, leader=%d] PutAppend, k=%s, v=%s, op=%s - other: %v",
					ck.clientId, leaderId, key, value, op, reply.Err)
			}
		} else {
			DPrintf("call KVServer.PutAppend failed")
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) currentLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leaderId
}

func (ck *Clerk) changeLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	return ck.leaderId
}
