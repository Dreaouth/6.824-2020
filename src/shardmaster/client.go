package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	// Your data here.
	leaderId	int
	clientId	int64
	lastReqId	int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.lastReqId = 0

	return ck
}

// 查询操作无需使用requestId
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	leaderId := ck.leaderId
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[leaderId].Call("ShardMaster.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = leaderId
			return reply.Config
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.lastReqId += 1
	args := &JoinArgs{
		Servers:   servers,
		ClientId:  ck.clientId,
		RequestId: ck.lastReqId,
	}
	leaderId := ck.leaderId
	for {
		var reply JoinReply
		ok := ck.servers[leaderId].Call("ShardMaster.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = leaderId
			return
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.lastReqId += 1
	args := &LeaveArgs{
		GIDs:      gids,
		ClientId:  ck.clientId,
		RequestId: ck.lastReqId,
	}
	leaderId := ck.leaderId
	for {
		var reply LeaveReply
		ok := ck.servers[leaderId].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = leaderId
			return
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.lastReqId += 1
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.clientId,
		RequestId: ck.lastReqId,
	}
	leaderId := ck.leaderId
	for {
		var reply MoveReply
		ok := ck.servers[leaderId].Call("ShardMaster.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leaderId = leaderId
			return
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
