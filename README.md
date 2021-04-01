## ShardKV设计思路

### ShardKV 可以优化的地方

1. 实现读写分离，可以通过牺牲一部分一致性实现读写分离，例如参考MVCC；如果在读写分离时保持强一致性，那就需要设计一个类似事务的功能，例如在写时进行一个预写提交，保证在对当前Key写时副本不能进行读取操作
2. 存储层，如何将数据更高效的持久化到硬盘中去
    1. 适用于多cpu场景
    2. 数据压缩
    3. 使用LSM（log-structured merge tree）树进行存储
3. Sharding Server的思路（使用一致性Hash进行分片）

### 重置election定时器的时机

- 从当前获取AppendEntries RPC调用 （如果outdata, term不一致，就不要reset timer）
- 开始leader选举
- 对其他candidate RPC请求进行投票

## Lab2 Part A

### Bug分析

在进行TestReElection2A测试时，当disconnect(leader)后又重新connect(leader)，此时进行checkOneLeader()会检测出有两个leader，导致test failed。经过日志排查后发现，在经过disconnect(leader)后，并不是关闭leader，而是模拟出一种网络分区的状态，leader依旧运行，只是leader和其他节点无法通过RPC通信。

所以，在disconnect(leader)后，leader发送AppendEntries超时，导致PRC阻塞，当connect后依旧阻塞，所以无法再次发送AppendEntries到新的leader并根据新leader的term退回到follower状态。

这里我的做法是，将每个发送并处理PRC请求的函数，都使用一个新的协程发送，这样不会因为网络分区问题超时而阻塞，每次AppendEntriesTImer到时都会新建线程发送。这样有一个缺点是可能在网络分区发生后，系统中有大量阻塞的协程，每个协程交互分到时间片会降低系统运行的效率，还可能造成PRC请求乱序，这里有待解决。看别人的代码发现他们有的会单独设计一个RPCTimer，来自主定义RPC超时时间，从而解决超时阻塞问题。

```go
for i,_ := range rf.peers {
   if i == rf.me {
      continue
   }
   go func(x int) {
      for rf.state == Leader {
         select {
         case <- rf.stopCh:
            return
         case <- rf.appendEntryTime[x].C:
            go rf.startAppendEntry(x)
         }
      }
   }(i)
}
```

startElection()部分也是同理。在disconnect(follower)后，follower由于长时间没收到heartBeats导致electionTimeout，重新转化为Candidate进行leader选举，当发送RequestVote后，系统重新让其connect()，此时RequestVoteRequest超时，导致无法接收其他消息和再次进行Leader选举。所以经过排查后使用协程 go startElection()

## Lab2 Part B

### 设计架构

- KVServer通过GetState函数判断当前Server是否为Leader；KVServer通过Start函数传递Command，然后raft判断是否是leader，如果是leader就添加日志
- 在raft Server启动时，系统会自动运行一个后台协程，检测electionTimer超时后进行leader选举。在Leader选举时，raft首先从follower状态切换至candidate状态，然后开启新协程向每个server发送RequestVote RPC，同时，自己通过channel检测投票结果，如果投票结果超过半数，就成为Leader
- 当changeState为Leader后，开始定时向其他follower发送AppendEntries，根据接收到的AppendEntries如果超过半数，则进行commitLogs
- Leader收到AppendEntries的reply，根据reply.Term判断返回False的RPC请求属于HeartBeat还是AppendEntries，如果是follower和leader的nextIndex不符，需要回退nextIndex
- follower通过根据Leader发送的AppendEntries中的PrevLogIndex和PrevLogTerm判断是否需要追加日志
- 对于提交日志(CommitLogs)部分，Leader当检测到发往各个follower的AppendEntries超过半数都返回成功，则新开一个协程进行commitLogs；对于Follower来说，当Leader进行commitLog后，会更新commitLogs，并在下一次AppnedEntries时发送给follower，当follower检测到rf.lastApplied < args.LeaderCommit时，就进行commitLogs和Leader对齐

### Bug分析

- logs启动时初始化长度为1，commitIndex和lastApplied依照论文初始化为0。将logs初始化长度为1，logs[0]为空，是因为第一次发送AppendEntries时要指明rf.logs[prevLogIndex].Term，如果日志长度为0会报错，当然也可以多加一步判断
- 在Leader的AppendEntries的reply中，需要判断reply.Term和rf.currentTerm，之前没有进行判断，当进行HeartBeats时，leaderTerm大于currentTerm时，follower会直接修改currentTerm并返回，这时reply.Term小于leader的currentTerm，而且reply.NextIndex=0，如果直接rf.nextIndex[server] = reply.NextIndex会有问题，所以必须加一步判断reply.Term == rf.currentTerm

```go
if reply.Term > rf.currentTerm {
   rf.currentTerm = reply.Term
   rf.changeState(Follower)
   rf.persist()
   return
} else if reply.Term == rf.currentTerm { 
   rf.nextIndex[server] = reply.NextIndex
   rf.persist()
   return
}
```

- 通过跳过一个Term而不是只跳过一个index进行加速。如下：

```go
// 3. If an existing entry conflicts with a new one (different terms), delete the existing entry and all that follow it
if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
    ...
    for nextIndex > rf.commitIndex && term == rf.logs[nextIndex].Term {
        nextIndex -= 1
    }
}
```
之前的方法是判断 args.PrevLogTerm != rf.logs[nextIndex].Term 但是测试时发现当args.PrevLogTerm大于该follower的的Term时，无论nextIndex怎么减1都无法匹配，导致每次都减为0。所以参考其他人做法，尝试跳过一个Term。即取follower的rf.logs[args.PrevLogIndex].Term，然后从args.PrevLogIndex往前遍历（减少循环，因为PrevLogIndex后面的一定不匹配），当该Term发生变化或小于commitIndex时，停止遍历，返回nextIndex。相当于论文所说的 delete the existing entry and all that follow it

- 如下：

```go
if reply.Success {
   DPrintf("Server %v get server %v's startAppendEntry reply success %+v", rf.me, server, reply)
   if reply.NextIndex > rf.nextIndex[server] {
      rf.nextIndex[server] = reply.NextIndex
      // bug修复：可能存在RPC乱序 再多考虑一下
      // rf.matchIndex[server] = reply.NextIndex - 1
      rf.matchIndex[server] = prevLogIndex + len(args.Entries)
   }
   ...
```
可能存在RPC乱序的问题，导致后一个reply先于前一个到达从而影响了rf.matchIndex，进而影响了commitLogs

- commitLogs需要将包含Command的命令提交到ApplyMsg的channel，是一个耗时操作，所以要新开一个协程

## Lab2 Part C

### 设计架构

- Part C部分比较简单，就是在Part B的基础上增加了持久化部分，将raft的currentTerm、votedFor、logs持久化，并在每次修改时执行rf.persist()进行持久化


## Lab3 Part A

### 设计架构

Lab3A部分是要基于Raft协议实现一个容错KV服务，通过client发送Put()、Append()、Get()。KVServer将操作(Op)提交给raft，raft的log保存Put()、Append()、Get()操作，当raft进行commitLog后，KVServer会收到applyMsg，并按顺序执行log中的操作，应用到它们的KV数据库

#### Client

- Client负责通过Clerk将Put()、Append()、Get()通过RPC发送给Server
- 增加leaderId字段。在冷启动或切换Leader时，Client不知道哪个Server对应的raft是Leader，所以需要遍历将请求依次通过RPC发给其他Server，当收到正确回应时，将leaderId字段设置为当前leader，以免下次请求继续寻找leader

#### Server

- 在KVServer启动时，启动一个协程waitApplyCh，不断获取raft通过applyCh中传来的applyMsg，对其进行处理。并通过唯一的(clientId, requestId) 判断是否重复并执行 kv.lastApplies[op.ClientID] = op.RequestId，如果不是重复的，就在数据库中执行操作并将结果通过channel传送给applyCommand
- KVServer接收Get和PutAppend请求，对于Get请求，如果当前数据库中存在，则直接查找并返回，不用执行applyCommand的步骤。如果暂时不存在（可能还没commit），则需要重新封装请求为一个Op并执行applyCommand
- applyCommand通过kv.rf.Start(op)将操作提交，并新建一个channel用于接收waitApplyCh的结果并返回给Client。结果类型如下：

```go
const (
   OK             = "OK"
   ErrWrongLeader = "ErrWrongLeader"
   ErrTimeOut    = "ErrTimeOut"
)
```

### bug分析

- 开始时，我理解错了Append的意思，导致最后一个case怎么也过不了。之前我的做法是如果数据库没有这个key就返回Err，正确的做法是如果数据库没有这个key就直接Put，和Put操作的作用一致
- 在applyCommand中，

```go
if _, ok := kv.msgNotify[index]; !ok {
   // 这里对于Leader，每次新操作时都会新生产一个channel，如果没人来取，就根本放不进去，从而导致阻塞，所以改成带buffer的chan
   kv.msgNotify[index] = make(chan NotifyMsg, 1)
}
```
为什么要用这种方式而不是新建一个channel呢，通过start函数传来的index做map的key，并在每次要新建时判断是否存在，保证了一个请求只能被raft执行一次

- 在applyCommand接收到waitApplyCh的请求时，之前没有进行判断，导致partition的case过不去，其实是在发生网络分区的情况下，，新的leader和旧的leader都执行了commit，导致别的请求通过一个channel发过来，这时应该返回错误并进行重发

```go
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
```

- 在waitApplyCh中判断请求是否重复时，之前是判断 kv.isRepeated(op.ClientID, op.RequestId 如果存在重复就直接返回 continue，这种方式会遇到一个问题：1.当检测到了重复请求，如果直接return的话，由于重复请求已经提交并从applyMsg返回，但本函数中没有将结果通过channel传回 (ch <- NotifyMsg)， 所以在applyCommand中只会检测到超时请求，进而返回timeout，所以要在case <- t.C中判断是否属于重复请求，但这种方式只能是基于超时判断，效率不高。所以我的方案还是将重复请求也通过channel传送给NotifyMsg，只是在判断Op时检测到重复请求就不改变数据库

```go
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
```

## Lab3 Part B

### 设计架构

实现KVServer和Raft的快照功能。当Raft的log不断增长的过程中，可能会导致日志过大而超出内存发生OOM，所以需要实现snapshot功能来截断一部分日志（日志压缩 log compact）。当KVServer重启的时候，会首先从persister读取snapshot然后恢复之前的状态。

实现思路：当KVServer端（Leader）发现当前raft的raft_state_size接近max_raft_state时触发taskSnapshot，taskSnapshot先将本KVServer端的db与lastApplies编码并打包起来发送到raft端，raft端再将其log截断，然后将当前状态（包括发送来KVServer端的）持久化。并遍历当前raft集群中的节点（Follower），分别发送startInstallSnapshot RPC请求

其他Follower获取RPC请求，根据请求中的lastIncludeTerm和lastIncludeIndex来截断log并更新commitIndex和lastApplied。最后，新建一个ApplyMsg，将CommandValid设置为false表示installSnapshot，通过rf.applyCh发送给KVServer（属于Follower端的KVServer）进行installSnapshot

### bug修复

- 当Leader向其他Follower发送InstallSnapshot RPC后，并返回成果后，需要将Leader自己的nextIndex和matchIndex更新

```java
if rf.sendSnapShot(server, &args, &reply) {
   rf.mu.Lock()
   if reply.Term > rf.currentTerm {
      rf.currentTerm = reply.Term
      rf.changeState(Follower)
      rf.persist()
   } else {
      // 这里容易忽略
      if rf.matchIndex[server] < args.LastIncludeIndex {
         rf.matchIndex[server] = args.LastIncludeIndex
      }
      rf.nextIndex[server] = rf.matchIndex[server] + 1
   }
   rf.mu.Unlock()
}
```
 - 在每次进行changeState(Follower)将状态转化为Follower时，需要进行resetElectionTimer。
 这样一个逻辑，当一个follower发生了网络分区，这边正常的server（leader、follower）进行正常交互。但发生分区的那个follower由于没有接收到appendEntry，导致不断的成为candidate，发送requestVote，然后term自增。当网络分区问题恢复后，由于之前发生分区的那个follower的term大，所以之前的leader回退到follower，但回退follower的leader由于log大于发生分区的follower，所以不会给它投票，最后还是之前是leader的follower重新开始选举并成为leader。所以在之前的leader回退到follower之后要进行resetElectionTimer(成为Leader后stop了electionTimer)，以便可以在下次选举过程中成为leader

 - 在Leader向Follower发送AppendEntries时，有这样一个情况。如果这个Leader已经进行过了snapShot，而一个follower掉线了，当Leader进行snapshot后又开始AppendEntries时，这个Follower又上线了，这时Leader发送AppendEntries时会进行检查，如果发生这种情况，就直接让Follower进行installSnapshot

```java
// 当其中一个raft server发生掉线或者网络分区会出现这种情况
if rf.nextIndex[server] - 1 < rf.lastSnapshotIndex {
   rf.unlock("startAppendEntry")
   rf.startInstallSnapshot(server)
   return
}
```

## Lab4 Part A

### 设计架构

![56e3513b25b6e11fb25baeb92050935f.png](en-resource://database/756:1)

一个集群只有Leader才能够正常服务，系统的性能和集群的数量成正比。Lab3是一个集群内的服务，Lab4A要实现的是多个集群之间的配合

Lab4就是要实现分库分表，将不同的数据划分到不同的集群上，保证相应数据请求引流到对应的集群。这里，将互不相交并且合力组成完整数据库的每一个数据库子集称为shard。在同一阶段中，shared与集群的对应关系称为配置（configuration），随着时间的推移，新集群的加入或者现有集群的离去，shared需要在不同集群之中进行迁移，如何处理好配置更新时shard的移动，是lab4的主要挑战

ShardMaster负责根据Client提供的分区规则，将数据存储在不同的replica group中。同时ShardMaster由多台机器组成，他们之间通过raft协议保持一致性。每一个replica group和Lab3的KVServer一样，由多台机器组成，他们之间也是通过Raft协议

Config的格式如下所示：

```go
type Config struct {
   Num    int              // config number
   Shards [NShards]int     // shard -> gid
   Groups map[int][]string // gid -> servers[]
}
```
configNum是当前Config的Number，每次Join、Leave、Move时，新建一个Config并将Num加1
shard 和 GID 是多对一的关系，GID 和 servers 是一对多的关系

**ShradMaster有如下4个API:**

- Join: 给出一组 GID->Servers的映射，就是把这些GID组，加到Master管理范围里来，那么有新的Group来了，每台机器可以匀一些Shard过去
- Leave: 给出一组GID，表示这组GID的机器要离开，那么ShardMaster原本由这些GID管理的Shard要匀给其他Group
- Move是指定某个Shard归这个GID管
- Query是根据Config Num来找对应的Config里的Shard规则是如何

#### Rebalance

在Join和Leave过程中，涉及到GID中Server的新增和Shard->GID的分配，所以需要用到rebalance操作。

首先根据之前的Shard->GID遍历，做一个GID->Shards的map映射。然后Join的话每次Shards数最大的GID，将其管理的Shard移到新加入的那个GID，直到整体GID管理的Shard达到平均。Leave的话每次找最小的GID，把Leave丢弃的GID中的Shards给当前Shards数最小的GID，一直到Leave的Group的Shard没有了

注意：这里不需要改变config.Groups，因为需要通过RPC传到每个KVServer，只改变master没有必要

### Bug修复&注意事项

- 在Join、Leave、Move过程中，ConfigNum+1，而Query时，没有对Config进行过改动，所以没有必要新建一个Config，所以需要对Query单独处理
- 在Join、Leave操作后，需要进行rebalance，以保证每个Shard都能分配到GID且达到平衡

## Lab4 Part B

### Client

Client在初始化时会传过来一个ShardMaster的Config参数，在进行Get和PutAppend请求时，先通过key得到它所属的Shard，每次按照当前Config中的Shard->GID得到GID，再根据Group[GID]得到当前Group的Servers，然后遍历Servers发送RPC请求直到返回成功（获取Leader），如果都没有请求成功说明当前配置不对，然后对ShardMaster进行一次Query得到最新的Config，再进行前面的操作

### Server

Server端当集群间当配置改变（GID->Shards）的时候，需要实现Shards的迁移。
- 如果一个Group丢失了一个Shards，它必须停止服务对丢失Shards的请求，并开始迁移需要迁移的数据
- 当一个Group获得了一个Shards，它需要等待Shards之前的拥有者将其数据发过来，才可以接受关于该Shards的请求

```go
type ShardKV struct {
   // Your definitions here.
   mck             *shardmaster.Clerk
   cfg             shardmaster.Config
   persist          *raft.Persister
   stopCh       chan struct{}
   msgNotify     map[int]chan Op
   lastApplies       map[int64]int
   db              map[string]string

   // 新增的部分
   toOutShards       map[int]map[int]map[string]string  // cfg num -> (shard -> DB)
   comeInShards   map[int]int       // shard -> config num
   ownShards     map[int]bool   // 当前可以提供服务的shard（进行pull时需要pull进来的shard不能接受）
   garbages      map[int]map[int]bool // cfg num -> shards
}
```
**toOutShards为什么要使用cfgNum的键值对对应out的数据：**

因为ShardKV在通过applyInAndOutShard放到toOutShards,再到接收ShardMigration RPC请求将toOutShardsDB返回给发起请求RPC的Server这段过程中需要等待raft commitLog之后，tryPullShard并发送RPC，这段时间中（主要是RPC请求的时间）可能又一次更新了config，所以需要根据 cfg num 进行保存，在shardMigration时进行查询

**comeInShard  map[int]int**

而comeInShard存在的目的是为了让需要获取shard的server向其他server要shard，将之作为RPC的请求参数发送，必须按照config的顺序一次一次发送而且发送过程中不接收其他请求，所以不需要通过config num进行记录

### Server端设计架构

当新建一个ShardKVServer时，需要开启几个后台协程，除了在Lab3中已经说过的waitApplyCh，还有tryPullShard，tryPollNewConfig，tryGC三个线程

#### tryPollNewConfig()

每隔50ms就对ShardMaster进行一次Query，查询是否有需要更新的Config（cfgNum + 1），如果查到了新的Config，就将新的Config丢进Raft去同步（和Get、PutAppend一样）。

当Raft成功Commit这条数据并通过applyCh返回时，说明集群中所有节点都知道了需要更新config，开始解析这条Config内容。首先更新kv.config为新的Config，然后遍历新Config的shards，解析出comeInShards和toOutShards。如果toOutShards不为空，就遍历数据库将满足toOutShards的Shards删除

#### tryPullShard()

查询是否有新的需要向其他server要的shards（comeInShard），如果有，开启一个新协程，发送Migrate RPC请求到需要发送shards的Server

对方街道Migrate请求后，将自己的toOutShards和lastApplies做一份拷贝，然后返回

源发送方接收到发送成功请求后，和PollNewConfig一样，还是将返回的数据和lastApplies丢进Raft去进行同步。当Raft成功Commit这条数据并通过applyCh返回时，说明集群中所有节点都知道了需要更新lastApplies和DB，开始更新。同时将该config Num下的迁移过的shards放于garbageList（现在是comeInShards这边）

#### tryGC()

之前的实现中，新建了3个用于存储分库分表的数据结构ownShards、toOutShards、comeInShards

其中ownShards是固定大小的，comeInShards在接收方接收到MigrateReply后直接删除，并且是按照configNum递增的顺序进行检测（config.Num-1），不是则直接返回空。而toOutShards是发送shards方保存等待需要接收shards方给他发请求的，所以无法保证顺序，必须先用configNum做map (cfg num -> (shard -> DB))，而垃圾回收就是做这部分事情

如果是发送方接收MigrateArgs并发送reply后直接删除，是不可以的，因为这条消息有可能在RPC传送过程中消失，或者服务器接收后提交到raft最终没有commit，造成这个数据永远没有收到。

所以，为了防止RPC丢失，我的做法是和pullConfig一样，使用一个后台线程不断检测garbageList的内容，并向对应的group发送RPC（因为是在comeInShards方成功地处理了MigrateReply才放进garbageList）。接收方（toOutShards）收到请求时，将请求打包成一个Command丢进Raft，然后等待集群中所有节点达成一致并成功删除toOutShards中具体ConfigNum的内容，向发送方返回成功。最后，发送方接受到回复后，将自己的garbageList删除