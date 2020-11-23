package shardkv

import (
	"../shardmaster"
	"strconv"
	"sync"
)

/*   在之前的实现中，新建了3个用于存储分库分表的数据结构，ownShards、toOutShards、comeInShards
其中ownShards是固定大小的，comeInShards在接收方接收到MigrateReply后直接删除，并且是按照configNum递增的顺序进行检测（config.Num-1），不是则直接返回空
而toOutShards是发送shards方保存等待需要接收shards方给他发请求的，所以无法保证顺序，必须先用configNum做map (cfg num -> (shard -> DB))，而垃圾回收就是做这部分事情
     如果是发送方接收MigrateArgs并发送reply后直接删除，是不可以的，因为这条消息有可能在RPC传送过程中消失，或者服务器接收后提交到raft最终没有commit，
造成这个数据永远没有收到。所以，为了防止RPC丢失，我的做法是和pullConfig一样，使用一个后台线程不断检测garbageList的内容，并向对应的group发送RPC
     具体做法是，当接收shards方发送的请求收到回复并成功commitLog后，把收到的这个reply信息发送给garbageList，后台GC线程检测到garbageList有内容时，
就会向其对应的group发送GC RPC，如果对应的group中的toOutShards清理成功，就发送成功reply，然后就把garbageList对应的内容删除
 */


func (kv *ShardKV) applyGC(op *Op) {
	//有没有可能在接收到GCArgs后，提交到raft，并等到日志commit这段时间中，toOutShards又接收到新的shard，导致删除了不该删除的请求呢
	cfgNum, _ := strconv.Atoi(op.Key)
	shard := op.RequestId
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.toOutShards[cfgNum]; ok {
		delete(kv.toOutShards[cfgNum], shard)
		if len(kv.toOutShards[cfgNum]) == 0 {
			delete(kv.toOutShards, cfgNum)
		}
		DPrintf("ShardKV %v-%v delete toOutShard cfgNum %v shard %v", kv.gid, kv.me, cfgNum, shard)
	}
}


func (kv *ShardKV) GarbageCollection(args *GCArgs, reply *GCReply)  {
	DPrintf("ShardKV %v-%v receive gc args %+v", kv.gid, kv.me, args)
	reply.Err = ErrWrongLeader
	_, isLeader := kv.rf.GetState()
	if !isLeader {return}
	kv.mu.Lock()
	if _, ok := kv.toOutShards[args.ConfigNum]; !ok {
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.toOutShards[args.ConfigNum][args.Shard]; !ok {
		kv.mu.Unlock()
		return
	}
	// 将gc的消息当作一个op提交给raft
	op := Op{
		Key:       strconv.Itoa(args.ConfigNum),
		Value:     "",
		Type:      "GC",
		ClientId:  nrand(),
		RequestId: args.Shard,
	}
	kv.mu.Unlock()
	reply.Err, _ = kv.applyCommand(op)
	DPrintf("ShardKV %v-%v successful apply raft log", kv.gid, kv.me)
}


func (kv *ShardKV) tryGC()  {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.garbages) == 0 {
		kv.mu.Unlock()
		return
	}
	DPrintf("ShardKV %v-%v start to ask for gc, kv.garbages is %+v", kv.gid, kv.me, kv.garbages)
	var wait sync.WaitGroup
	for cfgNum, shards := range kv.garbages {
		for shard := range shards {
			wait.Add(1)
			go func(shard int, config shardmaster.Config) {
				defer wait.Done()
				args := GCArgs{
					Shard:     shard,
					ConfigNum: config.Num,
				}
				gid := config.Shards[shard]
				for _, server := range config.Groups[gid] {
					srv := kv.make_end(server)
					reply := GCReply{}
					ok := srv.Call("ShardKV.GarbageCollection", &args, &reply)
					if ok && reply.Err == OK {
						kv.mu.Lock()
						defer kv.mu.Unlock()
						delete(kv.garbages[cfgNum], shard)
						if len(kv.garbages[cfgNum]) == 0 {
							delete(kv.garbages, cfgNum)
						}
						DPrintf("ShardKV %v-%v successful get gc reply and delete kv.garbage", kv.gid, kv.me)
					}
				}
			}(shard, kv.mck.Query(cfgNum))
		}
	}
	kv.mu.Unlock()
	wait.Wait()
}