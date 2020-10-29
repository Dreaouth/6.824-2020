package raft

// implement heartbeats
type AppendEntriesArgs struct {
	Term		 int
	LeaderID	 int
	PrevLogIndex int
	PrevLogTerm  int
	Entries		 []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term		 int
	Success		 bool
	// 增加一个NextIndex字段，用于快速同步日志，减少RPC通信量
	NextIndex	 int
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	DPrintAppendEntriesArgs(rf.me, args)
	rf.lock("AppendEntriesHandler")
	defer rf.unlock("AppendEntriesHandler")
	reply.Term = rf.currentTerm
	reply.Success = false
	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(Follower)
		rf.resetElectionTimer()
		rf.persist()
	}

	rf.currentTerm = args.Term
	rf.changeState(Follower)
	rf.resetElectionTimer()
	rf.persist()

	lastLogIndex := len(rf.logs) - 1
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		DPrintf("Server %v this situation should not be happened", rf.me)
		reply.Success = false
		reply.NextIndex = rf.lastSnapshotIndex + 1
		return
	} else if args.PrevLogIndex == rf.lastSnapshotIndex && (args.PrevLogIndex + len(args.Entries)) > rf.lastSnapshotIndex {
		DPrintf("Server %v sync log to leader %v", rf.me, args.LeaderID)
		reply.Success = true
		rf.logs = rf.logs[:1]
		rf.logs = append(rf.logs, args.Entries...)
		lastLogIndex = len(rf.logs) - 1
		reply.NextIndex = rf.getAbsoluteLogIndex(lastLogIndex + 1)
		rf.persist()  // ???
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getAbsoluteLogIndex(lastLogIndex) {
		reply.NextIndex = rf.getAbsoluteLogIndex(lastLogIndex + 1)
		DPrintf("Server %v args.PrevLogIndex > lastLogIndex, lastLogIndex is %v, reply nextIndex is %v", rf.me, lastLogIndex, lastLogIndex + 1)
		return
	}

	// 3. If an existing entry conflicts with a new one (different terms), delete the existing entry and all that follow it
	if args.PrevLogTerm != rf.logs[rf.getRelativeLogIndex(args.PrevLogIndex)].Term { // 尴尬的bug 之前是 rf.logs[lastLogIndex].Term
		// bug修复：之前的方法是判断 args.PrevLogTerm != rf.logs[nextIndex].Term 但是测试时发现当args.PrevLogTerm大于该follower的
		// 的Term时，无论nextIndex怎么减1都无法匹配，导致每次都减为0。所以参考其他人做法，尝试跳过一个Term
		// 即取follower的rf.logs[args.PrevLogIndex].Term，然后从args.PrevLogIndex往前遍历（减少循环，因为PrevLogIndex后面的一定不匹配），
		// 当该Term发生变化或小于commitIndex时，停止遍历，返回nextIndex。相当于论文所说的 delete the existing entry and all that follow it
		nextIndex := args.PrevLogIndex
		term := rf.logs[rf.getRelativeLogIndex(args.PrevLogIndex)].Term
		for nextIndex > rf.commitIndex && nextIndex > rf.lastSnapshotIndex && term == rf.logs[rf.getRelativeLogIndex(nextIndex)].Term {
			nextIndex -= 1
		}
		DPrintf("Server %v args.PrevLogTerm != rf.logs[lastLogIndex].Term, lastLogIndex is %v, reply nextIndex is %v", rf.me, lastLogIndex, nextIndex + 1)
		reply.NextIndex = nextIndex + 1
		return
	}

	// 4. Append any new entries not already in the log
	if args.PrevLogTerm == rf.logs[rf.getRelativeLogIndex(args.PrevLogIndex)].Term { // 这里也是一个和刚才一样的小bug
		DPrintf("Server %v args.PrevLogTerm == rf.logs[lastLogIndex].Term", rf.me)
		reply.Success = true
		// bug修复：应为 rf.logs[:args.PrevLogIndex + 1] 而不是 rf.logs
		rf.logs = append(rf.logs[:rf.getRelativeLogIndex(args.PrevLogIndex + 1)], args.Entries...)
		rf.persist()
		reply.NextIndex = rf.getAbsoluteLogIndex(len(rf.logs))

		if rf.lastApplied+1 <= args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit // 与leader同步信息
			go rf.commitLogs()
		}

		// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			DPrintf("leaderCommit > commitIndex")
			rf.commitIndex = min(args.LeaderCommit, rf.getAbsoluteLogIndex(len(rf.logs) - 1))
		}
	}
	reply.Success = true
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Server %v send a AppendEntry request to %v {Term:%v PrevLogIndex:%v PrevLogTerm:%v LeaderCommit:%v}", rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	if !ok {
		DPrintf("Server %v send append entries to server %v failed", rf.me, server)
	}
	return ok
}


func (rf *Raft) resetAppendEntriesTimer()  {
	for i,_ := range rf.peers {
		rf.appendEntryTimer[i].Stop()
		rf.appendEntryTimer[i].Reset(HeartBeatTimeout)
	}
}


func (rf *Raft) resetAppendEntryTimer(server int)  {
	rf.appendEntryTimer[server].Stop()
	rf.appendEntryTimer[server].Reset(HeartBeatTimeout)
}


// Leader向follower发送
func (rf *Raft) startAppendEntry(server int)  {
	rf.lock("startAppendEntry")
	if rf.state != Leader {
		rf.resetAppendEntriesTimer()
		rf.unlock("startAppendEntry")
		DPrintf("Server %v is not leader now, close the startAppendEntry to server %v", rf.me, server)
		return
	}
	rf.resetAppendEntryTimer(server)
	prevLogIndex := rf.nextIndex[server] - 1

	// 当其中一个raft server发生掉线或者网络分区会出现这种情况
	if prevLogIndex < rf.lastSnapshotIndex {
		rf.unlock("startAppendEntry")
		rf.startInstallSnapshot(server)
		return
	}

	var entries []LogEntry
	// bug修复，根据nextIndex判断是否有log需要同步，还是仅仅发送HeartBeats
	if rf.nextIndex[server] < rf.getAbsoluteLogIndex(len(rf.logs)) {
		entries = make([]LogEntry, len(rf.logs[rf.getRelativeLogIndex(prevLogIndex + 1):]))
		copy(entries, rf.logs[rf.getRelativeLogIndex(prevLogIndex + 1):])
	}
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderID: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: rf.logs[rf.getRelativeLogIndex(prevLogIndex)].Term,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.unlock("startAppendEntry")
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(server, &args, &reply)

	rf.lock("AppendEntries.Reply")
	defer rf.unlock("AppendEntries.Reply")

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.changeState(Follower)
		rf.resetElectionTimer()
		rf.persist()
		return
	}

	if reply.Success {
		DPrintf("Server %v get server %v's startAppendEntry reply success %+v", rf.me, server, reply)
		if reply.NextIndex > rf.nextIndex[server] {
			rf.nextIndex[server] = reply.NextIndex
			// bug修复：可能存在RPC乱序 再多考虑一下
			// rf.matchIndex[server] = reply.NextIndex - 1
			rf.matchIndex[server] = prevLogIndex + len(args.Entries)
		}
		// 当获取到AppendEntry消息，日志追加相同的server超过集群中的一半时，进行commitLog
		count := 1
		for i := 0; i < len(rf.peers) ; i++ {
			if rf.me == i {
				continue
			}
			if rf.matchIndex[i] == rf.matchIndex[server] {
				count += 1
			}
		}
		// 当HeartBeat时无需commitLog
		if count > len(rf.peers)/2 && rf.commitIndex < rf.matchIndex[server] {
			rf.commitIndex = rf.matchIndex[server]
			go rf.commitLogs()
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.changeState(Follower)
			rf.persist()
			DPrintf("Server %v get server %v's startAppendEntry reply failed %v, down to follower", rf.me, server, reply)
			return
		} else if reply.Term == rf.currentTerm { // 加一个判断，因为HeartBeats的reply.NextIndex是0，不应该直接设置为reply.NextIndex
			// 这里不应该直接执行一个新的appendEntries，而是根据设置nextIndex，在下一次AppendEntries时根据nextIndex[server]进行发送
			DPrintf("Server %v get server %v's startAppendEntry reply failed, resend %v", rf.me, server, reply)
			rf.nextIndex[server] = reply.NextIndex
			rf.persist()
			return
		}
		DPrintf("Server %v get server %v's startAppendEntry reply failed %v", rf.me, server, reply)
	}
	return
}


func (rf *Raft) startAppendEntries()  {
	// 尝试在changeState为leader后启动
	// leader定时发送 AppendEntries(HeartBeats)
	for i,_ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(x int) {
			for rf.state == Leader {
				select {
				case <- rf.stopCh:
					return
				case <- rf.appendEntryTimer[x].C:
					go rf.startAppendEntry(x)
				}
			}
		}(i)
	}
}
