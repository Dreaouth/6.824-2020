package shardkv

import (
	"../shardmaster"
	"sync"
)

/*
	主要是分析是谁应该去migrate的问题。当一个Group A检查到多了一个shard，对应Group B就要失去一个shard，此时由于A需要等待得到一个新的shard（通过RPC），所以A是不能响应请求的，
但B需要丢弃一个shard，当检测到更新时，可以直接丢弃掉这个shard，然后响应其他请求
	基于以上考虑，决定当A检测到shard增加时，发送RPC去GroupB要shard，当A pull成功，更新config才可以继续接收请求。而B发现了新config，可以直接更新并生效
	在所有的通信过程中，不能让state machine自己参与通信，因为无法确保server不会在通信过程中挂掉。所以还是借助raft commitLog的方式实现，这样整个系统的状态就是可控的了
 */

// 需要发送shard的Server接收到请求，将自己要发送的shards做一个deepCopy，然后
func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	reply.Err, reply.Shard, reply.ConfigNum = ErrWrongLeader, args.Shard, args.ConfigNum
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !isLeader {
		return
	}
	if args.ConfigNum >= kv.cfg.Num {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err = OK
	reply.DB, reply.LastApplies = kv.deepCopyDBAndLastApplies(args.ConfigNum, args.Shard)
	DPrintf("ShardKV %v-%v receive a shardMigration request, args is %+v, origin db is %+v, reply db is %+v", kv.gid, kv.me, args, kv.db, reply.DB)
}


func (kv *ShardKV) deepCopyDBAndLastApplies(num int, shard int) (map[string]string, map[int64]int) {
	db := make(map[string]string)
	lastApplies := make(map[int64]int)
	for k, v := range kv.toOutShards[num][shard] {
		db[k] = v
	}
	for k, v := range kv.lastApplies {
		lastApplies[k] = v
	}
	return db, lastApplies
}

// 当Server从commitLog接收到新的config时，和自己的config，ownShards作比较，并解析出自己需要向其他Server索要的shards和向其他Server发送的shards
func (kv *ShardKV) applyInAndOutShard(config shardmaster.Config)  {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("ShardKV %v-%v config is %+v", kv.gid, kv.me, kv.cfg)
	DPrintf("ShardKV %v-%v receive new config is %+v", kv.gid, kv.me, config)
	if config.Num <= kv.cfg.Num {
		return
	}
	oldCfg, toOutShards := kv.cfg, kv.ownShards
	kv.cfg, kv.ownShards = config, make(map[int]bool)
	// 搜索新的config和以前的config比较
	for shard, gid := range config.Shards {
		if gid != kv.gid {
			continue
		}
		// 新的config中已经包括了旧的部分
		if _, ok := toOutShards[shard]; ok || oldCfg.Num == 0 {
			DPrintf("ShardKV %v-%v ownShards %v is equal to new config's shards", kv.gid, kv.me, shard)
			kv.ownShards[shard] = true
			delete(toOutShards, shard)
		} else {	// 新的config没有包括需要发送RPC向其他Group要新的shard
			DPrintf("ShardKV %v-%v get new shard %v and need to ask for other servers", kv.gid, kv.me, shard)
			kv.comeInShards[shard] = oldCfg.Num
		}
	}
	// 剩下还没有delete掉的，说明是要发送给其他group（等待其他group向自己要 MigrateArgs）
	if len(toOutShards) > 0 {
		// 重新初始化针对该configNum的shard -> DB
		// bug修复：之前是将toOutShards全部初始化了
		kv.toOutShards[oldCfg.Num] = make(map[int]map[string]string)
		for shard := range toOutShards {
			outDB := make(map[string]string)
			for k, v := range kv.db {
				if key2shard(k) == shard {
					outDB[k] = v
					delete(kv.db, k)
				}
			}
			kv.toOutShards[oldCfg.Num][shard] = outDB
			DPrintf("ShardKV %v-%v should remove shard %v and save shard to toOutShards", kv.gid, kv.me, shard)
		}
	}
}

// 需要获取shards的Server收到了raft commitLogs，表示集群中其他服务器达成一致，开始更新数据库
func (kv *ShardKV) applyMigrateData(migrateData MigrateReply)  {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if migrateData.ConfigNum != kv.cfg.Num -1 {
		return
	}
	// bug修复4B：kv.comeInShards (shard -> config num) ，之前写成了 delete(kv.comeInShards, migrateData.ConfigNum)
	// 导致不会删除已经更新的comeInShards，后台进程不断tryPullShard，检测到有comeInShard然后一直发送RPC请求，并提交到raft日志
	delete(kv.comeInShards, migrateData.Shard)

	if _, ok := kv.ownShards[migrateData.Shard]; !ok {
		kv.ownShards[migrateData.Shard] = true
		for k, v := range migrateData.DB {
			kv.db[k] = v
		}
		for k, v := range migrateData.LastApplies {
			kv.lastApplies[k] = Max(v, kv.lastApplies[k])
		}
	}
	DPrintf("ShardKV %v-%v successful update migrateData shard is %v, migrate db is %+v", kv.gid, kv.me, migrateData.Shard, migrateData.DB)
	DPrintf("ShardKV %v-%v own shards is %+v", kv.gid, kv.me, kv.ownShards)
	DPrintf("ShardKV %v-%v db is %+v", kv.gid, kv.me, kv.db)
}


// 不断 Query shardMaster并判断是否有需要更新的config
func (kv *ShardKV) tryPollNewConfig()  {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.comeInShards) > 0 {
		kv.mu.Unlock()
		return
	}
	nextCfgNum := kv.cfg.Num + 1
	// bug修复：这里要直接unlock，否则会出现 ShardKV 和 Raft 三重锁
	kv.mu.Unlock()
	newCfg := kv.mck.Query(nextCfgNum)
	if newCfg.Num == nextCfgNum {
		DPrintf("ShardKV %v-%v got new config and poll to raft server, config is %+v", kv.gid, kv.me, newCfg)
		kv.rf.Start(newCfg)
	}
}

// 查询是否有新的需要向其他server要的shards（comeInShard），并发送RPC请求到需要发送shards的Server，目标Server接收请求并返回了data，然后自己将reply data通过raft进行同步
func (kv *ShardKV) tryPullShard()  {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.comeInShards) == 0 {
		kv.mu.Unlock()
		return
	}
	DPrintf("ShardKV %v-%v start to pull comeInShards(ask other server for shards)", kv.gid, kv.me)
	DPrintf("ShardKV %v-%v comeInShards is %+v", kv.gid, kv.me, kv.comeInShards)
	var wait sync.WaitGroup
	for shard, idx := range kv.comeInShards {
		wait.Add(1)
		// 后台协程为什么需要 kv.mck.Query(idx)
		// 因为已经更新了config，所以需要Query旧的config
		go func(shard int, cfg shardmaster.Config) {
			defer wait.Done()
			args := MigrateArgs{
				Shard:     shard,
				ConfigNum: cfg.Num,
			}
			gid := cfg.Shards[shard]
			for _, server := range cfg.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
				// bug修复4B：之前是reply.Err != ErrWrongLeader
				if ok && reply.Err == OK {
					DPrintf("ShardKV %v-%v start a ShardMigration apply shard %v to raft server %v", kv.gid, kv.me, reply.Shard, server)
					kv.rf.Start(reply)
				} else {
					DPrintf("ShardKV %v-%v receive wrong leader from server %v, shard %v", kv.gid, kv.me, server, reply.Shard)
				}
			}
		}(shard, kv.mck.Query(idx))
	}
	kv.mu.Unlock()
	wait.Wait()
}
