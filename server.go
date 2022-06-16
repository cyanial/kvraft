package kvraft

import (
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
	store map[string]string

	waitApplyCh  map[int]chan Op
	lastApplySeq map[int64]int64

	snapshotIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[Server %d] Get, k=%s -", kv.me, args.Key)

	op := Op{
		Key:         args.Key,
		Value:       "",
		Method:      "Get",
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("[Server %d] Get, k=%s - Wrong Leader", kv.me, args.Key)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	indexCh, exist := kv.waitApplyCh[index]
	if !exist {
		kv.waitApplyCh[index] = make(chan Op)
		indexCh = kv.waitApplyCh[index]
	}
	kv.mu.Unlock()

	select {
	case commitOp := <-indexCh:
		if commitOp.ClientId == op.ClientId && commitOp.SequenceNum == op.SequenceNum {
			kv.mu.Lock()
			value, has := kv.store[op.Key]
			kv.lastApplySeq[op.ClientId] = op.SequenceNum
			kv.mu.Unlock()
			if has {
				DPrintf("[Server %d] Get, k=%s - OK", kv.me, args.Key)
				reply.Err = OK
				reply.Value = value
			} else {
				DPrintf("[Server %d] Get, k=%s - No Key", kv.me, args.Key)
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			DPrintf("[Server %d] Get, k=%s - Not match", kv.me, args.Key)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(Server_Timeout):
		DPrintf("[Server %d] Get, k=%s - Timeout", kv.me, args.Key)

		_, isLeader := kv.rf.GetState()
		if kv.isDuplicatedCmd(op.ClientId, op.SequenceNum) && isLeader {
			kv.mu.Lock()
			value, has := kv.store[op.Key]
			kv.lastApplySeq[op.ClientId] = op.SequenceNum
			kv.mu.Unlock()

			if has {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, index)
	kv.mu.Unlock()
	close(indexCh)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[Server %d] PutAppend, k=%s, v=%s, op=%s -",
		kv.me, args.Key, args.Value, args.Op)

	op := Op{
		Key:         args.Key,
		Value:       args.Value,
		Method:      args.Op,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("[Server %d] PutAppend, k=%s, v=%s, op=%s - Wrong Leader",
			kv.me, args.Key, args.Value, args.Op)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	indexCh, exist := kv.waitApplyCh[index]
	if !exist {
		kv.waitApplyCh[index] = make(chan Op)
		indexCh = kv.waitApplyCh[index]
	}
	kv.mu.Unlock()

	select {
	case commitOp := <-indexCh:
		if commitOp.ClientId == op.ClientId && commitOp.SequenceNum == op.SequenceNum {
			DPrintf("[Server %d] PutAppend, k=%s, v=%s, op=%s - OK",
				kv.me, args.Key, args.Value, args.Op)
			reply.Err = OK
		} else {
			DPrintf("[Server %d] PutAppend, k=%s, v=%s, op=%s - Not match",
				kv.me, args.Key, args.Value, args.Op)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(Server_Timeout):
		DPrintf("[Server %d] PutAppend, k=%s, v=%s, op=%s - Timeout",
			kv.me, args.Key, args.Value, args.Op)
		if kv.isDuplicatedCmd(op.ClientId, op.SequenceNum) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, index)
	kv.mu.Unlock()
	close(indexCh)
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

		select {
		case applyMsg := <-kv.applyCh:

			switch {

			case applyMsg.CommandValid:

				if applyMsg.CommandIndex <= kv.snapshotIndex {
					break
				}

				op := applyMsg.Command.(Op)

				if !kv.isDuplicatedCmd(op.ClientId, op.SequenceNum) {
					switch op.Method {

					case "Get":

					case "Put":
						DPrintf("[Server %d] Apply Put, k=%v, v=%v",
							kv.me, op.Key, op.Value)

						kv.mu.Lock()
						kv.store[op.Key] = op.Value
						kv.lastApplySeq[op.ClientId] = op.SequenceNum
						kv.mu.Unlock()
					case "Append":
						DPrintf("[Server %d] Apply Append, k=%v, v=%v",
							kv.me, op.Key, op.Value)

						kv.mu.Lock()
						// to-do: impl
						kv.store[op.Key] += op.Value
						kv.lastApplySeq[op.ClientId] = op.SequenceNum
						kv.mu.Unlock()
					}
				}

				if kv.maxraftstate != -1 {
					if kv.rf.RaftStateSize() > kv.maxraftstate {
						snapshot := kv.CreateSnapshot()
						kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
					}
				}
				// reply
				DPrintf("[Server %d] %v, apply OK",
					kv.me, op.Method)
				kv.mu.Lock()
				indexCh, exist := kv.waitApplyCh[applyMsg.CommandIndex]
				if exist {
					indexCh <- op
				}
				kv.mu.Unlock()
			case applyMsg.SnapshotValid:
				DPrintf("[Server %d] snapshot, apply", kv.me)
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					kv.InstallSnapshot(applyMsg.Snapshot)
					kv.snapshotIndex = applyMsg.SnapshotIndex
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

	kv := &KVServer{
		me:            me,
		maxraftstate:  maxraftstate,
		applyCh:       make(chan raft.ApplyMsg),
		store:         make(map[string]string),
		waitApplyCh:   make(map[int]chan Op),
		lastApplySeq:  make(map[int64]int64),
		snapshotIndex: 0,
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	if persister.SnapshotSize() > 0 {
		// install snapshot
		DPrintf("Init Server - Install Snapshot: %d", kv.me)
		kv.InstallSnapshot(persister.ReadSnapshot())
	}

	go kv.applier()

	DPrintf("Init Server: %d", kv.me)

	return kv
}
