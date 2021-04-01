package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"../labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)


const (
	ElectionTimeout  = time.Millisecond * 300
	HeartBeatTimeout = time.Millisecond * 120
	MaxLockTime      = time.Millisecond * 10  // debug
)

type State int

const (
	Follower	State = 0
	Leader 		State = 1
	Candidate	State = 2
)



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int

	CommandSnapshot	[]byte
}

type LogEntry struct {
	Term 		int
	Index  		int
	Command 	interface{}
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]
	dead      	int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	stopCh				chan struct{}
	applyCh				chan ApplyMsg

	state				State
	currentTerm			int
	votedFor 			int
	logs 				[]LogEntry
	commitIndex 		int
	lastApplied 		int
	nextIndex			[]int  // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex			[]int  // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	lastSnapshotIndex 	int

	appendEntryTimer	[]*time.Timer
	electionTimer		*time.Timer
	lockStart			time.Time	// for debug
	lockEnd				time.Time
	lockName			string
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.lock("GetState")
	isLeader = rf.state == Leader
	term = rf.currentTerm
	defer rf.unlock("GetState")
	if isLeader {
		DPrintf("Server %v get state is leader: %v", rf.me, isLeader)
	}
	return term, isLeader
}



func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastSnapshotIndex)
	data := w.Bytes()
	return data
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor 	int
	var logs 		[]LogEntry
	var commitIndex int
	var lastSnapshotIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&commitIndex) != nil  || d.Decode(&lastSnapshotIndex) != nil {
		panic("fail to decode state")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.commitIndex = commitIndex
		rf.lastSnapshotIndex = lastSnapshotIndex
		// bug修复： raft重启时要将rf.lastApplied初始化为lastSnapshotIndex，否则在commitLog时会报错（index为负数）
		rf.lastApplied = lastSnapshotIndex
		rf.mu.Unlock()
	}
}


func (rf *Raft) lock(m string)  {
	rf.mu.Lock()
	rf.lockName = m
	rf.lockStart = time.Now()
	// DPrintf("lock %v is locking\n", m)
}


func (rf *Raft) unlock(m string) {
	rf.lockEnd = time.Now()
	rf.lockName = m
	//duration := rf.lockEnd.Sub(rf.lockStart)
	//if rf.lockName != "" && duration > MaxLockTime {
	//	fmt.Printf("lock too long:%s:%s:iskill:%v\n", m, duration, rf.killed())
	//}
	rf.mu.Unlock()
}


func (rf *Raft) changeState(state State) {
	switch state {
	case Follower:
		rf.state = state
		/* 3B问题：之前忘加了，这样一个逻辑，当一个follower发生了网络分区，这边正常的server（leader、follower）进行正常交互。但发生分区的那个follower由于没有
		接收到appendEntry，导致不断的成为candidate，发送requestVote，然后term自增。当网络分区问题恢复后，由于之前发生分区的那个follower的term大，所以之前的
		leader回退到follower，但回退follower的leader由于log大于发生分区的follower，所以不会给它投票，最后还是之前是leader的follower重新开始选举并成为leader
		 */
		rf.resetElectionTimer()
	case Candidate:
		rf.state = state
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.resetElectionTimer()
		DPrintf("Server %v convert to candidate\n", rf.me)
	case Leader:
		rf.state = state
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.getAbsoluteLogIndex(len(rf.logs))
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = rf.lastSnapshotIndex   // 3B，开始时是0
		}
		rf.electionTimer.Stop()
		rf.startAppendEntries()
		rf.resetAppendEntriesTimer()
		DPrintf("Server %v convert to leader\n", rf.me)
	}
}


// 因为是新开的一个协程，所以需要加锁
func (rf *Raft) commitLogs() {
	rf.lock("commitLogs")
	if rf.commitIndex > rf.getAbsoluteLogIndex(len(rf.logs) - 1) {
		DPrintf("Commit logs error, rf.commitIndex > len(rf.logs) - 1")
	}

	// 提交到applyCh，以便KVServer可以存到数据库
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{CommandIndex: i, Command: rf.logs[rf.getRelativeLogIndex(i)].Command, CommandValid: true}
	}

	rf.lastApplied = rf.commitIndex
	DPrintf("Server %v Commit Logs success, lastApplied is %v, commitIndex is %v", rf.me, rf.lastApplied, rf.commitIndex)
	rf.unlock("commitLogs")
}


func (rf *Raft) randElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}


func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) getRelativeLogIndex(index int) int {
	return index - rf.lastSnapshotIndex
}

func (rf *Raft) getAbsoluteLogIndex(index int) int {
	return index + rf.lastSnapshotIndex
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.lock("Start")
	defer rf.unlock("Start")
	isLeader = rf.state == Leader
	term = rf.currentTerm
	if isLeader {
		rf.logs = append(rf.logs, LogEntry{
			Term: rf.currentTerm,
			Index: rf.getAbsoluteLogIndex(len(rf.logs)),
			Command: command,
		})
		index = rf.getAbsoluteLogIndex(len(rf.logs) - 1)
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
		DPrintf("Server %v start an command to be appended to Raft's log, log's last term is %v, index is %v, log length is %v",
			rf.me, rf.logs[len(rf.logs) - 1].Term, rf.logs[len(rf.logs) - 1].Index, len(rf.logs))
	}

	return index, term, isLeader
}




//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.stopCh)
	DPrintf("Server %v Kill\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}



//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.stopCh = make(chan struct{})
	rf.applyCh = applyCh
	rf.electionTimer = time.NewTimer(rf.randElectionTimeout())
	rf.appendEntryTimer = make([]*time.Timer, len(peers))
	for i,_ := range rf.peers {
		rf.appendEntryTimer[i] = time.NewTimer(HeartBeatTimeout)
	}

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	// 初始时日志长度为1
	rf.logs = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastSnapshotIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// 发起投票
	go func() {
		for {
			select {
			case <- rf.stopCh:
				return
			case <- rf.electionTimer.C:
				go rf.startElection()
			}
		}
	}()

	return rf
}
