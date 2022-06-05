package kvraft

import "log"

const Debug = false

const Server_TimeoutMS = 1000

func init() {
	log.SetFlags(log.Ltime)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
