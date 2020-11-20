package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "../labrpc"
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

const ChangeLeaderInterval = time.Millisecond * 10

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	// lastReqId使用自增int而不是随机数，保证请求顺序
	leaderId	int
	clientId	int64
	lastReqId	int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.lastReqId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.lastReqId = ck.lastReqId + 1
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.RequestId = ck.lastReqId
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			// bug修复：这里一开始想简单了，要根据该group进行一个一个的查询
			for i := 0; i < len(servers); i++ {
				si := (i + ck.leaderId) % len(servers)
				DPrintf("Client %v start to send Get request to server %v-%v", ck.clientId, gid, si)
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.Err == OK {
					ck.leaderId = si
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.lastReqId = ck.lastReqId + 1
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.lastReqId,
	}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for i := 0; i < len(servers); i++ {
				si := (i + ck.leaderId) % len(servers)
				DPrintf("Client %v start to send PutAppend request %+v= to server %v-%v", ck.clientId, args, gid, si)
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.leaderId = si
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
