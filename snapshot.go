package kvraft

import (
	"bytes"

	"github.com/cyanial/raft"
	"github.com/cyanial/raft/labgob"
)

type PersistSnapshot struct {
	Store        map[string]string
	LastApplySeq map[int64]int64
}

func (kv *KVServer) GetSnapShotFromRaft(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
		kv.InstallSnapshot(applyMsg.Snapshot)
		kv.snapshotIndex = applyMsg.SnapshotIndex
	}
	kv.mu.Unlock()
}

func (kv *KVServer) CreateSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	snapshot := PersistSnapshot{
		Store:        kv.store,
		LastApplySeq: kv.lastApplySeq,
	}

	err := e.Encode(snapshot)
	if err != nil {
		DPrintf("kv.CreateSnapshot failed")
		return nil
	}

	return w.Bytes()
}

func (kv *KVServer) InstallSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistSnapshot PersistSnapshot

	err := d.Decode(&persistSnapshot)
	if err != nil {
		DPrintf("kv.InstallSnapshot failed")
		return
	}

	kv.store = persistSnapshot.Store
	kv.lastApplySeq = persistSnapshot.LastApplySeq
}
