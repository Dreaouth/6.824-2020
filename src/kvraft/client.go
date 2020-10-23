package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

const ChangeLeaderInterval = time.Millisecond * 20

type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	// You will have to modify this struct.
	// lastReqId使用自增int而不是随机数，保证请求顺序
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.lastReqId = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	requestId := ck.lastReqId + 1
	args := GetArgs{
		Key:      	key,
		RequestId:  requestId,
		ClientId: 	ck.clientId,
	}
	leaderId := ck.leaderId
	for {
		DPrintf("Client %v send get request to server %v request is %v", ck.clientId, leaderId, args)
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("Client %v get server %v err", ck.clientId, leaderId)
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		} else {
			switch reply.Err {
			case OK:
				DPrintf("Client %v get server %v success", ck.clientId, leaderId)
				ck.leaderId = leaderId
				ck.lastReqId = requestId
				return reply.Value
			case ErrWrongLeader:
				DPrintf("Client %v get server %v error wrong leader", ck.clientId, leaderId)
				leaderId = (leaderId + 1) % len(ck.servers)
				continue
			case ErrTimeOut:
				// 重试
				DPrintf("Client %v get server %v error time out", ck.clientId, leaderId)
				time.Sleep(ChangeLeaderInterval)
				continue
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	requestId := ck.lastReqId + 1
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		RequestId:requestId,
	}
	leaderId := ck.leaderId
	for {
		DPrintf("Client %v send PutAppend request to server %v request is %v", ck.clientId, leaderId, args)
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			DPrintf("Client %v get PutAppend server %v err", ck.clientId, leaderId)
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		} else {
			switch reply.Err {
			case OK:
				DPrintf("Client %v get PutAppend server %v success", ck.clientId, leaderId)
				ck.leaderId = leaderId
				ck.lastReqId = requestId
				return
			case ErrWrongLeader:
				DPrintf("Client %v get PutAppend server %v error wrong leader", ck.clientId, leaderId)
				leaderId = (leaderId + 1) % len(ck.servers)
				continue
			case ErrTimeOut:
				// 重试
				DPrintf("Client %v get server %v error time out", ck.clientId, leaderId)
				time.Sleep(ChangeLeaderInterval)
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
