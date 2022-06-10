package kvraft

import (
	"log"
	"time"
)

const Debug = false

const Server_Timeout = 1000 * time.Millisecond

func init() {
	log.SetFlags(log.Ltime)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) isDuplicatedCmd(clientId, sequenceNum int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastApplyNum, hasClient := kv.lastApplySeq[clientId]
	if !hasClient {
		// has no client
		return false
	}
	return sequenceNum <= lastApplyNum
}
