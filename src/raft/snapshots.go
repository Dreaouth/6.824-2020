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
	Offset				int
	Data[]				[]byte
	Done				bool
}

type InstallSnapshotReply struct {
	Term		int
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply)  {
	DPrintf("Server %d get InstallSnapshot request %+v", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

}