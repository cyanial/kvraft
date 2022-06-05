package kvraft

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyanial/raft"
	"github.com/cyanial/raft/labgob"
	"github.com/cyanial/raft/labrpc"
)

type Op struct {
	Key         string
	Value       string
	Method      string
	ClientId    int64
	SequenceNum int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store *KVStore

	cmdCommitCh map[string]chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	cmd := Op{
		Key:         args.Key,
		Value:       "",
		Method:      "Get",
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	DPrintf("[Server %d] Get, k=%s -", kv.me, args.Key)
	_, _, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		DPrintf("[Server %d] Get, k=%s, - WrongLeader", kv.me, args.Key)
		reply.Err = ErrWrongLeader
		return
	}

	name := strconv.Itoa(int(args.ClientId)) + strconv.Itoa(int(args.SequenceNum))
	cmdCommit := make(chan struct{})
	kv.mu.Lock()
	kv.cmdCommitCh[name] = cmdCommit
	kv.mu.Unlock()

	select {
	case <-cmdCommit:
		value, has := kv.store.Get(args.Key)
		if has {
			DPrintf("[Server %d] Get, k=%s, - OK", kv.me, args.Key)
			reply.Err = OK
			reply.Value = value
		} else {
			DPrintf("[Server %d] Get, k=%s, - No Key", kv.me, args.Key)
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	case <-time.After(Server_TimeoutMS * time.Millisecond):
		DPrintf("[Server %d] Get, k=%s, - Timeout", kv.me, args.Key)
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.cmdCommitCh, name)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	cmd := Op{
		Key:         args.Key,
		Value:       args.Value,
		Method:      args.Op,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	DPrintf("[Server %d] PutAppend, k=%s, v=%s, op=%s -",
		kv.me, args.Key, args.Value, args.Op)

	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		DPrintf("[Server %d] PutAppend, k=%s, v=%s, op=%s - Wrong Leader",
			kv.me, args.Key, args.Value, args.Op)
		reply.Err = ErrWrongLeader
		return
	}

	name := strconv.Itoa(int(args.ClientId)) + strconv.Itoa(int(args.SequenceNum))
	cmdCommit := make(chan struct{})
	kv.mu.Lock()
	kv.cmdCommitCh[name] = cmdCommit
	kv.mu.Unlock()

	select {
	case <-cmdCommit:
		DPrintf("[Server %d] PutAppend, k=%s, v=%s, op=%s - OK",
			kv.me, args.Key, args.Value, args.Op)
		reply.Err = OK
	case <-time.After(Server_TimeoutMS * time.Millisecond):
		DPrintf("[Server %d] PutAppend, k=%s, v=%s, op=%s - Timeout",
			kv.me, args.Key, args.Value, args.Op)
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.cmdCommitCh, name)
	kv.mu.Unlock()

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		for applyMsg := range kv.applyCh {
			if applyMsg.CommandValid {
				cmd := applyMsg.Command.(Op)
				DPrintf("[Server %d] applyMsg: m=%s", kv.me, cmd.Method)
				switch cmd.Method {
				case "Get":

				case "Put":
					kv.store.Put(cmd.Key, cmd.Value)
				case "Append":
					kv.store.Append(cmd.Key, cmd.Value)
				}

				name := strconv.Itoa(int(cmd.ClientId)) + strconv.Itoa(int(cmd.SequenceNum))
				kv.mu.Lock()
				ch, has := kv.cmdCommitCh[name]
				if has {
					ch <- struct{}{}
				}
				kv.mu.Unlock()
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &KVServer{}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = MakeStore()
	kv.cmdCommitCh = map[string]chan struct{}{}

	go kv.applier()

	DPrintf("Init Server: %d", kv.me)

	return kv
}
