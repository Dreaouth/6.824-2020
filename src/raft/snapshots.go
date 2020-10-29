package raft

/*
	from time to time kvserver will persistently store a "snapshot" of its current state,
	and Raft will discard log entries that precede the snapshot.When a server restarts
	(or falls far behind the leader and must catch up), the server first installs a snapshot
	and then replays log entries from after the point at which the snapshot was created.

	Whenever your key/value server detects that the Raft state size is approaching this threshold,
	it should save a snapshot, and tell the Raft library that it has snapshotted, so that Raft can discard old log entries.
*/

type InstallSnapshotArgs struct {
	Term				int
	LeaderId			int
	LastIncludeIndex	int
	LastIncludeTerm		int
	Data				[]byte
}

type InstallSnapshotReply struct {
	Term		int
}


// 这里我的策略是在KVServer执行SavePersistAndSnapshot时，先在Persist中
func (rf *Raft) SavePersistAndSnapshot(logIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %v start to SavePersistAndSnapshot", rf.me)
	if logIndex <= rf.lastSnapshotIndex {
		return
	}
	if logIndex > rf.commitIndex {
		panic("Server %v SavePersistAndSnapshot logIndex > rf.commitIndex")
	}
	// 截断log
	rf.logs = rf.logs[rf.getRelativeLogIndex(logIndex):]
	rf.lastSnapshotIndex = logIndex
	persistData := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(persistData, snapshot)
	
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.startInstallSnapshot(i)
	}
}


func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply)  {
	DPrintf("Server %d get InstallSnapshot request", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludeIndex <= rf.lastSnapshotIndex {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(Follower)
	}
	lastIncludedRelativeIndex := rf.getRelativeLogIndex(args.LastIncludeIndex)
	DPrintf("Server %v lastIncludedRelativeIndex is %v, len(rf.logs) is %v", rf.me, lastIncludedRelativeIndex, len(rf.logs))
	// 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	if len(rf.logs) > lastIncludedRelativeIndex && rf.logs[lastIncludedRelativeIndex].Term == args.LastIncludeTerm {
		rf.logs = rf.logs[lastIncludedRelativeIndex:]
	} else {
		// 7. Discard the entire log
		rf.logs = []LogEntry{{Term: args.LastIncludeTerm, Index: args.LastIncludeIndex, Command: nil}}
	}
	rf.lastSnapshotIndex = args.LastIncludeIndex
	// 因为是从Leader的KV层判断大小是否超过snapshotSize，然后进行snapshot，理论情况下commitIndex和lastApplied已经更新，
	// 但考虑网络分区情况下（某个follower在commit前发生网络分区，然后在snapshot时恢复），还要做一个检查
	if rf.commitIndex < rf.lastSnapshotIndex {
		rf.commitIndex = rf.lastSnapshotIndex
	}
	if rf.lastApplied < rf.lastSnapshotIndex {
		rf.lastApplied = rf.lastSnapshotIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)

	if rf.lastApplied > rf.lastSnapshotIndex {
		return
	}

	// 发送snapshot到follower的KVServer
	installSnapshotCommand := ApplyMsg{
		CommandValid:    false,
		Command:         nil,
		CommandIndex:    rf.lastSnapshotIndex,
		CommandSnapshot: rf.persister.ReadSnapshot(),
	}
	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(installSnapshotCommand)
}


func (rf *Raft) startInstallSnapshot(server int)  {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastSnapshotIndex,
		LastIncludeTerm:  rf.logs[0].Term,
		Data:             rf.persister.ReadSnapshot(),
	}
	DPrintf("Server %d start to send Snapshot to server %d", rf.me, server)
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	if rf.sendSnapShot(server, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.changeState(Follower)
			rf.persist()
		} else {
			// 这里容易忽略
			if rf.matchIndex[server] < args.LastIncludeIndex {
				rf.matchIndex[server] = args.LastIncludeIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
		rf.mu.Unlock()
	}
}


func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotHandler", args, reply)
	if !ok {
		DPrintf("Server %v send snapshot to server %v failed", rf.me, server)
	}
	return ok
}