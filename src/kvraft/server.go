package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)



const WaitCommandInterval = time.Millisecond * 500

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key 		string
	Value		string
	Type		string
	ClientID	int64
	RequestId	int
}

type NotifyMsg struct {
	Err			Err
	ClientId	int64
	RequestId	int
}

type KVServer struct {
	mu      	sync.Mutex
	me      	int
	rf      	*raft.Raft
	applyCh 	chan raft.ApplyMsg
	dead    	int32 	 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	msgNotify 	map[int]chan NotifyMsg
	lastApplies	map[int64]int
	db 			map[string]string
	stopCh		chan struct{}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server %v receive a Get request from client %v", kv.me, args.ClientId)
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
		ClientID:  args.ClientId,
		RequestId: args.RequestId,
	}
	res := kv.applyCommand(op)
	reply.Err = res.Err
	kv.mu.Lock()
	value, ok := kv.db[args.Key]
	kv.mu.Unlock()
	if ok {
		reply.Value = value
	}
	DPrintf("server %v get key:%s, err:%s, value:%s", kv.me, op.Key, reply.Err, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("server %v receive a PutAppend request from client %v", kv.me, args.ClientId)
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClientID:  args.ClientId,
		RequestId: args.RequestId,
	}
	res := kv.applyCommand(op)
	reply.Err = res.Err
	DPrintf("server %v get key:%s, err:%s", kv.me, op.Key, reply.Err)
}


func (kv *KVServer) applyCommand(op Op) (res NotifyMsg) {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return res
	}
	kv.mu.Lock()
	if _, ok := kv.msgNotify[index]; !ok {
		// 这里对于Leader，每次新操作时都会新生产一个channel，如果没人来取，就根本放不进去，从而导致阻塞，所以改成带buffer的chan
		kv.msgNotify[index] = make(chan NotifyMsg, 1)
	}
	ch := kv.msgNotify[index]
	DPrintf("Server %v start applyCommand requestId:%v index:%v, term:%v", kv.me, op.RequestId, index, term)
	kv.mu.Unlock()
	t := time.NewTimer(WaitCommandInterval)
	defer t.Stop()
	select {
	case res = <- ch:
		// bug修复：由于网络分区时，少数节点的leader也会进行操作。如果在一次start过后发生了网络分区，新的leader和旧的leader都执行了commit
		// 这时就需要检测applyCommand和返回的是否是一个request，如果不是的话，返回ErrWrongLeader，重新发送RPC到其他leader
		if res.RequestId != op.RequestId || res.ClientId != op.ClientID {
			DPrintf("Server %d, this situation is caused by net partition", kv.me)
			res.Err = ErrWrongLeader
		} else {
			res.Err = OK
		}
	case <- t.C:
		kv.mu.Lock()
		res.Err = ErrTimeOut
		kv.mu.Unlock()
	}
	kv.mu.Lock()
	delete(kv.msgNotify, index)
	kv.mu.Unlock()
	return res
}

func (kv *KVServer) waitApplyCh()  {
	for {
		select {
		case <- kv.stopCh:
			return
		case applyMsg := <- kv.applyCh:
			if !applyMsg.CommandValid {
				DPrintf("Server %v get applyMsg Command invalid, install snapshot", kv.me)
				continue
			}
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			// bug修复：之前是判断 kv.isRepeated(op.ClientID, op.RequestId 如果存在重复就直接返回 continue
			// 这种方式会遇到一个问题：1.当检测到了重复请求，如果直接return的话，由于重复请求已经提交并从applyMsg返回，但本函数中没有将结果通过channel传回 (ch <- NotifyMsg)
			// 所以在applyCommand中只会检测到超时请求，进而返回timeout，所以要在case <- t.C中判断是否属于重复请求，但这种方式只能是基于超时判断，效率不高
			// 所以我的方案还是将重复请求也通过channel传送给NotifyMsg，只是在判断Op时检测到重复请求就不改变数据库
			switch op.Type {
			case "Get":
				kv.lastApplies[op.ClientID] = op.RequestId
			case "Put":
				if !kv.isRepeated(op.ClientID, op.RequestId) {
					kv.db[op.Key] = op.Value
					kv.lastApplies[op.ClientID] = op.RequestId
				}
			case "Append":
				// bug修复：之前貌似是搞错了Append逻辑了，正确的应该是如果数据库中没有这个Key就直接append，和Put的作用一致
				if !kv.isRepeated(op.ClientID, op.RequestId) {
					kv.db[op.Key] += op.Value
					kv.lastApplies[op.ClientID] = op.RequestId
				}
			default:
				panic(fmt.Sprintf("unknown method: %s", op.Type))
			}
			// 检查apply前申请的channel是否存在（有可能不存在）
			if ch, ok := kv.msgNotify[applyMsg.CommandIndex]; ok {
				DPrintf("Server %v apply channel %v", kv.me, op)
				DPrintf("Server %v dp is %v", kv.me, kv.db)
				ch <- NotifyMsg{
					ClientId: 	op.ClientID,
					RequestId: 	op.RequestId,
				}
			}
			kv.mu.Unlock()
			DPrintf("kvserver %d applied command %s at index %d, request id %d, client id %d",
				kv.me, op.Type, applyMsg.CommandIndex, op.RequestId, op.ClientID)
		}
	}
}


// 当一个leader commit了一个entry到raft log中，然后该leader宕机了，这时applyCommand会检测到超时ErrTimeOut然后重发请求到其他leader
// 这里可能会导致一个request处理两遍，所以必须在server端判断请求是否重复。通过唯一的(clientID, requestID)来标记每一个操作
func (kv *KVServer) isRepeated(clientId int64, requestId int) bool {
	val, ok := kv.lastApplies[clientId]
	if ok == false || requestId > val {
		return false
	}
	return true
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.lastApplies = make(map[int64]int)
	kv.msgNotify = make(map[int]chan NotifyMsg)
	kv.db = make(map[string]string)
	kv.stopCh = make(chan struct{})

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.waitApplyCh()

	return kv
}
