package shardkv

import (
	"../labrpc"
	"../shardmaster"
	"bytes"
	"fmt"
	"time"
)
import "../raft"
import "sync"
import "../labgob"


/*
	You will need to make your servers watch for configuration changes, and when one is detected, to start the shard migration process.
	If a replica group loses a shard, it must stop serving requests to keys in that shard immediately, and start migrating the data for
that shard to the replica group that is taking over ownership.
	If a replica group gains a shard, it needs to wait for the previous owner to send over the old shard data before accepting requests
for that shard.
 */

const WaitCommandInterval = time.Millisecond * 500

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key			string
	Value 		string
	Type		string
	ClientId	int64
	RequestId	int
}


type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck				*shardmaster.Clerk
	cfg				shardmaster.Config
	persist			*raft.Persister
	stopCh		 	chan struct{}
	msgNotify	 	map[int]chan Op
	lastApplies		map[int64]int
	db 			 	map[string]string

	/* 因为ShardKV在通过applyInAndOutShard放到toOutShards,再到接收ShardMigration RPC请求将toOutShardsDB返回给发起请求RPC的Server这段过程中
	需要等待raft commitLog之后，tryPullShard并发送RPC，这段时间中（主要是RPC请求的时间）可能又一次更新了config，所以需要根据 cfg num 进行保存，
	在shardMigration时进行查询
	   而comeInShard存在的目的是为了让需要获取shard的server向其他server要shard，将之作为RPC的请求参数发送，必须按照config的顺序一次一次发送
	而且发送过程中不接收其他请求，所以不需要通过config num进行记录
	*/
	toOutShards		map[int]map[int]map[string]string	// cfg num -> (shard -> DB)
	comeInShards	map[int]int		// shard -> config num
	ownShards		map[int]bool	// 当前可以提供服务的shard（进行pull时需要pull进来的shard不能接受）
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("ShardKV %v-%v receive a Get request from client %v", kv.gid, kv.me, args.ClientId)
	// 针对Get请求判断当前数据库是否存在，如果存在直接返回，省去了applyCommand的步骤
	seq, ok := kv.lastApplies[args.ClientId]
	if ok && args.RequestId < seq {
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	op := Op{
		Key:       args.Key,
		Type:      "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.Err, reply.Value = kv.applyCommand(op)
	DPrintf("ShardKV %v-%v get Get key:%s, err:%s, value:%s", kv.gid, kv.me, op.Key, reply.Err, reply.Value)

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("ShardKV %v-%v receive a PutAppend request from client %v", kv.gid, kv.me, args.ClientId)
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.Err, _ = kv.applyCommand(op)
	DPrintf("ShardKV %v-%v get PutAppend key:%s, value:%s err:%s", kv.gid, kv.me, op.Key, op.Value, reply.Err)
}


func (kv *ShardKV) matchShard(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.cfg.Shards[key2shard(key)] == kv.gid
}


func (kv *ShardKV) checkSnapshot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1 {
		return false
	}
	if kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		return true
	}
	return false
}


// 写入snapshot时（Leader），在KV层开始执行snapshot然后通知raft层进行snapshot
func (kv *ShardKV) takeSnapshot(logIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.lastApplies)
	e.Encode(kv.toOutShards)
	e.Encode(kv.comeInShards)
	e.Encode(kv.ownShards)
	e.Encode(kv.cfg)
	data := w.Bytes()
	kv.mu.Unlock()
	kv.rf.SavePersistAndSnapshot(logIndex, data)
}


func (kv *ShardKV) installSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db 				map[string]string
	var lastApplies 	map[int64]int
	var toOutShards		map[int]map[int]map[string]string
	var comeInShards	map[int]int
	var ownShards		map[int]bool
	var cfg				shardmaster.Config
	// bug修复：decode的顺序要和encode的一致
	if d.Decode(&db) != nil || d.Decode(&lastApplies) != nil || d.Decode(&toOutShards) != nil || d.Decode(&comeInShards) != nil ||
		d.Decode(&ownShards) != nil || d.Decode(&cfg) != nil {
		panic("fail to decode state")
	} else {
		kv.db = db
		kv.lastApplies = lastApplies
		kv.ownShards = ownShards
		kv.comeInShards = comeInShards
		kv.toOutShards = toOutShards
		kv.cfg = cfg
		DPrintf("Install Snapshot, ShardKV %v db is %v", kv.me, kv.db)
	}
}

func (kv *ShardKV) daemon(run func(), sleepMs int)  {
	for {
		select {
		case <- kv.stopCh:
			return
		default:
			run()
		}
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
}


func (kv *ShardKV) applyCommand(op Op) (err Err, value string) {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		err = ErrWrongLeader
		return err, ""
	}
	kv.mu.Lock()
	if _, ok := kv.msgNotify[index]; !ok {
		kv.msgNotify[index] = make(chan Op, 1)
	}
	ch := kv.msgNotify[index]
	DPrintf("ShardKV %v-%v start applyCommand requestId:%v index:%v, term:%v", kv.gid, kv.me, op.RequestId, index, term)
	kv.mu.Unlock()
	t := time.NewTimer(WaitCommandInterval)
	defer t.Stop()
	res := Op{}
	select {
	case res = <- ch:
		// bug修复：由于网络分区时，少数节点的leader也会进行操作。如果在一次start过后发生了网络分区，新的leader和旧的leader都执行了commit
		// 这时就需要检测applyCommand和返回的是否是一个request，如果不是的话，返回ErrWrongLeader，重新发送RPC到其他leader
		if res.RequestId == op.RequestId && res.ClientId == op.ClientId && res.Key == op.Key && res.Type == op.Type {
			err = OK
		} else if res.Type == ErrWrongGroup {
			err = ErrWrongGroup
		} else {
			err = ErrWrongLeader
		}
	case <- t.C:
		err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.msgNotify, index)
	kv.mu.Unlock()
	return err, res.Value
}


func (kv *ShardKV) waitApplyCh() {
	for {
		select {
		case <- kv.stopCh:
			return
		case applyMsg := <- kv.applyCh:
			if !applyMsg.CommandValid {
				DPrintf("ShardKV %v-%v get receive Command invalid, install snapshot", kv.gid, kv.me)
				kv.installSnapshot(applyMsg.CommandSnapshot)
				continue
			}
			if cfg, ok := applyMsg.Command.(shardmaster.Config); ok {
				DPrintf("ShardKV %v-%v receive a config update apply", kv.gid, kv.me)
				kv.applyInAndOutShard(cfg)
			} else if migrateReply, ok := applyMsg.Command.(MigrateReply); ok {
				DPrintf("ShardKV %v-%v receive a migrate reply", kv.gid, kv.me)
				kv.applyMigrateData(migrateReply)
			} else {
				op := applyMsg.Command.(Op)
				kv.applyOp(&op)
				// raft log已经commit，检查是否需要快照
				if kv.checkSnapshot() {
					DPrintf("ShardKV %v-%v start to take a snapshot", kv.gid, kv.me)
					go kv.takeSnapshot(applyMsg.CommandIndex)
				}
				// 检查apply前申请的channel是否存在（对于Follower的ShardKV来说就不存在channel）
				if ch, ok := kv.msgNotify[applyMsg.CommandIndex]; ok {
					DPrintf("ShardKV %v-%v apply channel %v", kv.gid, kv.me, op)
					ch <- op
				}
				DPrintf("ShardKV %v-%d applied command %s at index %d, request id %d, client id %d", kv.gid,
					kv.me, op.Type, applyMsg.CommandIndex, op.RequestId, op.ClientId)
			}
		}
	}
}

func (kv *ShardKV) applyOp(op *Op) {
	shard := key2shard(op.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.ownShards[shard]; !ok {
		op.Type = ErrWrongGroup
		return
	}
	switch op.Type {
	case "Get":
		kv.lastApplies[op.ClientId] = op.RequestId
		op.Value = kv.db[op.Key]
	case "Put":
		if !kv.isRepeated(op.ClientId, op.RequestId) {
			kv.db[op.Key] = op.Value
			kv.lastApplies[op.ClientId] = op.RequestId
			DPrintf("ShardKV %v-%v db is %v", kv.gid, kv.me, kv.db)
		}
	case "Append":
		if !kv.isRepeated(op.ClientId, op.RequestId) {
			kv.db[op.Key] += op.Value
			kv.lastApplies[op.ClientId] = op.RequestId
			DPrintf("ShardKV %v-%v db is %v", kv.gid, kv.me, kv.db)
		}
	default:
		panic(fmt.Sprintf("unknown method: %s", op.Type))
	}
}


func (kv *ShardKV) isRepeated(clientId int64, requestId int) bool {
	val, ok := kv.lastApplies[clientId]
	if ok == false || requestId > val {
		return false
	}
	return true
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// bug修复：panic: runtime error: invalid memory address or nil pointer dereference [signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x58bf69]
	// 是由于ShardKV向raft提交更新的config导致的，在labgob注册后就不会报错了
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.stopCh = make(chan struct{})
	kv.persist = persister

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.cfg = shardmaster.Config{}
	kv.toOutShards = make(map[int]map[int]map[string]string)
	kv.comeInShards = make(map[int]int)
	kv.ownShards = make(map[int]bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.msgNotify = make(map[int]chan Op)
	kv.lastApplies = make(map[int64]int)
	kv.db = make(map[string]string)
	kv.installSnapshot(kv.persist.ReadSnapshot())

	go kv.waitApplyCh()
	go kv.daemon(kv.tryPollNewConfig, 50)
	go kv.daemon(kv.tryPullShard, 100)

	return kv
}
