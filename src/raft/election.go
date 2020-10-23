package raft


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term		 int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoteGranted bool
}


// example RequestVote RPC handler.
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("Server %v get a RequestVote request, candidate is %v, term is %v", rf.me, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term == rf.currentTerm {
		if rf.state == Leader {
			return
		}
		if  rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			return
		} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
			return
		}
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeState(Follower)
	}

	// Test 2B (5.4.1 election restriction)
	if args.LastLogTerm < rf.logs[len(rf.logs) - 1].Term || (args.LastLogTerm == rf.logs[len(rf.logs) - 1].Term && args.LastLogIndex < len(rf.logs) - 1){
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.changeState(Follower)
	reply.VoteGranted = true
	rf.resetElectionTimer()
	DPrintf("Server %v vote for %d\n", rf.me, args.CandidateId)
	return
}



//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) (bool) {
	DPrintf("Server %v send RequestVote request to %v on term %v", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	if !ok {
		DPrintf("Server %v Send request vote to server %v failed", rf.me, server)
	}
	return ok
}



func (rf *Raft) resetElectionTimer()  {
	rf.electionTime.Stop()
	rf.electionTime.Reset(rf.randElectionTimeout())
}


func (rf *Raft) startElection()  {
	rf.lock("Election")
	if rf.state == Leader {
		rf.unlock("Election")
		return
	}
	DPrintf("Server %v start Election\n", rf.me)
	rf.changeState(Candidate)
	rf.persist()
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm: rf.logs[len(rf.logs) - 1].Term,
	}
	rf.unlock("Election")
	voteCount := 1
	voteTrueNum := 1
	voteChannel := make(chan bool, len(rf.peers))
	for i,_ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(x int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(x, &args, &reply)
			rf.lock("down to follower")
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.changeState(Follower)
				rf.resetElectionTimer()
				rf.persist()
			}
			rf.unlock("down to follower")
			voteChannel <- reply.VoteGranted
		}(i)
	}

	for {
		vote := <- voteChannel
		voteCount += 1
		if vote == true {
			voteTrueNum += 1
		}
		if voteCount == len(rf.peers) || voteTrueNum > len(rf.peers)/2 || (voteCount - voteTrueNum) > len(rf.peers)/2 {
			break
		}
	}

	if voteTrueNum > len(rf.peers)/2 {
		rf.lock("switch to Leader")
		rf.changeState(Leader)
		rf.persist()
		rf.unlock("switch to Leader")
	}

}

