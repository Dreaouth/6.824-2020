package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintAppendEntriesArgs(server int, args *AppendEntriesArgs) {
	if Debug > 0 {
		log.Printf("Server %v get %v's AppendEntry request {Term:%v PrevLogIndex:%v PrevLogTerm:%v LeaderCommit:%v}",
			server, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		if len(args.Entries) > 0 {
			log.Printf("last log term is %v, index is %v, log length is %v",
				args.Entries[len(args.Entries) - 1].Term, args.Entries[len(args.Entries) - 1].Index, len(args.Entries))
		}
	}
}


func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}