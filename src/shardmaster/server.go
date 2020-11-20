package shardmaster

import (
	"../raft"
	"fmt"
	"math"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

/*
    一个集群只有Leader才能够正常服务，系统的性能和集群的数量成正比。Lab3是一个集群内的服务，Lab4A要实现的是多个集群之间的配合
	Lab4就是要实现分库分表，将不同的数据划分到不同的集群上，保证相应数据请求引流到对应的集群。这里，将互不相交并且合力组成完整数据库的每一个数据库子集称为shard。
在同一阶段中，shared与集群的对应关系称为配置（configuration），随着时间的推移，新集群的加入或者现有集群的离去，shared需要在不同集群之中进行迁移，如何处理好
配置更新时shred的移动，是lab4的主要挑战
 */

type Type string

const (
	Join 	Type = "Join"
	Leave	Type = "Leave"
	Move	Type = "Move"
	Query	Type = "Query"
)

const WaitCommandInterval = time.Millisecond * 500


type Op struct {
	// Your data here.
	Type		Type
	Args 		interface{}
	ClientId	int64
	RequestId	int
}


// 参考其他人的设计 Lab4换了一种风格，简化了RPC请求处理方面
type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs 	[]Config 		// indexed by config num
	stopCh		chan struct{}
	msgNotify 	map[int]chan Op
	lastApplies	map[int64]int
}

// bug修复4A：interface {} is *shardmaster.JoinArgs, not shardmaster.JoinArgs
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("ShardMaster %v receive a Join from client %v, args.Servers is %+v", sm.me, args.ClientId, args.Servers)
	op := Op{
		Type:      Join,
		Args:      *args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.WrongLeader = sm.applyCommand(op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("ShardMaster %v receive a Leave from client %v, args.GID is %+v", sm.me, args.ClientId, args.GIDs)
	op := Op{
		Type:      Leave,
		Args:      *args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.WrongLeader = sm.applyCommand(op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	DPrintf("ShardMaster %v receive a Move from client %v, args.GID is %+v args.Shard is %+v", sm.me, args.ClientId, args.GID, args.Shard)
	op := Op{
		Type:      Move,
		Args:      *args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.WrongLeader = sm.applyCommand(op)
}

// Query操作不会进行updateConfig，所以要单独处理
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	DPrintf("ShardMaster %v receive a Query, configNum is %v", sm.me, args.Num)

	// 加速Query
	reply.WrongLeader = true
	if _, isLeader := sm.rf.GetState(); !isLeader {
		return
	}
	sm.mu.Lock()
	if args.Num >= 0 && args.Num < len(sm.configs) {
		reply.WrongLeader, reply.Config = false, sm.configs[args.Num]
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	// bug修复4B：在测试4A时没问题，但到了4B就报错了，之前ClientId和RequestID还是和Join/Move/Leave一样，导致在进行Query时，会进行updateConfig
	// 新加入的config和之前的config一样，但config.num 增加了，ShardKV查询时会一直查到相同的config(num+1)，而不做任何变化，导致死循环
	op := Op{
		Type:      Query,
		Args:      *args,
		ClientId:  nrand(),
		RequestId: -1,
	}
	reply.WrongLeader = sm.applyCommand(op)
	if !reply.WrongLeader {
		sm.mu.Lock()
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Config = sm.configs[len(sm.configs) - 1]
		}
		sm.mu.Unlock()
	}
	DPrintf("ShardMaster %v Query reply config is %v", sm.me, reply)
}


func (sm *ShardMaster) applyCommand(op Op) bool {
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return true
	}
	sm.mu.Lock()
	var wrongLeader bool
	if _, ok := sm.msgNotify[index]; !ok {
		sm.msgNotify[index] = make(chan Op, 1)
	}
	ch := sm.msgNotify[index]
	DPrintf("ShardMaster %v start applyCommand %v requestId:%v index:%v", sm.me, op.Type, op.RequestId, index)
	sm.mu.Unlock()
	timer := time.NewTimer(WaitCommandInterval)
	defer timer.Stop()
	select {
	case res := <- ch:
		if res.RequestId != op.RequestId || res.ClientId != op.ClientId || res.Type != op.Type {
			wrongLeader = true
		} else {
			wrongLeader = false
		}
	case <- timer.C:
		wrongLeader = true
	}
	sm.mu.Lock()
	delete(sm.msgNotify, index)
	sm.mu.Unlock()
	return wrongLeader
}

func (sm *ShardMaster) waitApplyCh()  {
	for {
		select {
		case <- sm.stopCh:
			return
		case applyMsg := <- sm.applyCh:
			if !applyMsg.CommandValid {
				DPrintf("ShardMaster %v receive Command invalid, continue", sm.me)
				continue
			}
			op := applyMsg.Command.(Op)
			sm.mu.Lock()
			if !sm.isRepeated(op.ClientId, op.RequestId) && op.RequestId >= 0 {
				sm.updateConfig(op.Type, op.Args)
				sm.lastApplies[op.ClientId] = op.RequestId
			}
			//检查apply前申请的channel是否存在
			if ch, ok := sm.msgNotify[applyMsg.CommandIndex]; ok {
				DPrintf("ShardMaster %v apply channel %v", sm.me, op)
				ch <- op
			}
			sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) isRepeated(clientId int64, requestId int) bool {
	val, ok := sm.lastApplies[clientId]
	if ok == false || requestId > val {
		return false
	}
	return true
}

func (sm *ShardMaster) updateConfig(opType Type, arg interface{})  {
	config := sm.createNextConfig()
	switch opType {
	case Join:
		joinArg := arg.(JoinArgs)
		for gid, servers := range joinArg.Servers {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			config.Groups[gid] = newServers
			sm.rebalance(&config, opType, gid)
		}
	case Leave:
		leaveArg := arg.(LeaveArgs)
		for _, gid := range leaveArg.GIDs {
			delete(config.Groups, gid)
			sm.rebalance(&config, opType, gid)
		}
	case Move:
		moveArg := arg.(MoveArgs)
		if _, ok := config.Groups[moveArg.GID]; ok {
			config.Shards[moveArg.Shard] = moveArg.GID
		} else {
			return
		}
	default:
		panic(fmt.Sprintf("unknown method: %s", opType))
	}
	DPrintf("ShardMaster %v new config is %+v", sm.me, config)
	sm.configs = append(sm.configs, config)
}

// 将配置文件数加一，然后把之前的配置复制过来
func (sm *ShardMaster) createNextConfig() Config {
	lastConfig := sm.configs[len(sm.configs) - 1]
	nextConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,	// Shards数是预定义好的，所以直接复制即可
		Groups: make(map[int][]string),
	}
	for gid, servers := range lastConfig.Groups {
		nextConfig.Groups[gid] = append([]string{}, servers...)
	}
	return nextConfig
}

/*
	Join和Leave操作需要用到rebalance操作
	首先计算出之前的config中每个GID有几个Config，然后Join的话每次最大的，移到新加入的那个gid，直到达到平均。Leave的话找最小的，把Leave
丢弃的给最小的，一直到Leave的Group的shard没有了
	注意：这里不需要改变config.Groups，因为需要通过RPC传到每个KVServer，只改变master没有必要
 */
func (sm *ShardMaster) rebalance(config *Config, opType Type, gid int)  {
	shardCount := sm.groupByGid(config)
	switch opType {
	case Join:
		avg := NShards / len(config.Groups)
		for i := 0; i < avg; i++ {
			maxGid := sm.getMaxShardGid(shardCount)
			config.Shards[shardCount[maxGid][0]] = gid
			shardCount[maxGid] = shardCount[maxGid][1:]
		}
	case Leave:
		// 取出要删除的gid这一项
		shardsArray, exist := shardCount[gid]
		if !exist {
			return
		}
		delete(shardCount, gid)
		if len(config.Groups) == 0 {
			config.Shards = [NShards]int{}
			return
		}
		// 开始遍历删除项的所有server，并把server赋给含有最少gid的groups
		for _, v := range shardsArray {
			minGid := sm.getMinShardGid(shardCount)
			config.Shards[v] = minGid
			shardCount[minGid] = append(shardCount[minGid], v)
		}
	}
}

// 根据Shards [NShards]int，做一个gid到shard的map映射
func (sm *ShardMaster) groupByGid(config *Config) map[int][]int {
	shardCount := map[int][]int{}
	for gid := range config.Groups {
		shardCount[gid] = []int{}
	}
	for shard, gid := range config.Shards {
		shardCount[gid] = append(shardCount[gid], shard)
	}
	return shardCount
}

func (sm *ShardMaster) getMaxShardGid(shardCount map[int][]int) int {
	max := -1
	var gid int
	for k, v := range shardCount {
		if max <= len(v) {
			max = len(v)
			gid = k
		}
	}
	return gid
}

func (sm *ShardMaster) getMinShardGid(shardCount map[int][]int) int {
	min := math.MaxInt32
	var gid int
	for k, v := range shardCount {
		if min >= len(v) {
			min = len(v)
			gid = k
		}
	}
	return gid
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.stopCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.lastApplies = make(map[int64]int)
	sm.msgNotify = make(map[int]chan Op)
	sm.stopCh = make(chan struct{})

	go sm.waitApplyCh()

	return sm
}
