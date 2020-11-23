package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   		string
	Value 		string
	Op    		string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId	int64
	RequestId	int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key 		string
	// You'll have to add definitions here.
	ClientId	int64
	RequestId	int
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	Shard		int
	ConfigNum	int
}

/*
	4B:只是发SHARD DATA是不够的。比如一个APPEND REQUEST 在向A发送的时候，timeout了。这个时候A已经做了这个更新操作。在这个点之后，Reconfiguration 发生，
CLIENT 去问B发送APPEND REQ。如果只是SHARD DATA过去。会造成APPEND 2次。所以我们还需要把用于去重的map（LastApplies）也一起发过去。
 */
type MigrateReply struct {
	Err			Err
	ConfigNum	int
	Shard		int
	DB 			map[string]string
	LastApplies	map[int64]int
}

type GCArgs struct {
	Shard 		int
	ConfigNum	int
}

type GCReply struct {
	Err			Err
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}