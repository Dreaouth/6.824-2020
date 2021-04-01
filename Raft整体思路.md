### Leader选举

在Raft启动时，每个raft server自身都会维护一个指定范围内随机时间的election timer，开始时没有leader，当其中一个timer超时，就会开始进行leader选举

**Candidate:**

1. 首先，将自己的状态切换为Candidate，然后将currentTerm加1
2. 重置ElectionTimer定时器，并向自己投票
3. 构建RequestVote PRC，并分别开启新协程发送给除自己以外其他的server
4. 返回的参数中有term和voteGrated，返回的voteGrated表示是否投票给自己，如果投票失败，返回的term大于currentTerm，该server退回到Follower状态，并重置定时器
5. 如果投票成功，Candidate检测票数，当投票数超过了一半，自动成为leader，并向其他server发送AppendEntries

在Candidate发送RequestVote时，可能回收到来自其他Leader的AppendEntries（经过脑裂），如果Leader的Term大于等于该Candidate的Term，该Candidate就自动回退到Follower状态。否则回拒绝本次RPC并继续进行Leader选举

**Follower:**

1. 如果Candidate发送过来的Term小于自己的Term，则返回投票失败并将自己的term发送回去；如果Candidate发送过来的Term大于自己的Term，退化为Follower（防止两个不一样Term的Candidate一直进行Leader选举然后永远选出Leader）
2. 如果Candidate发送过来的Term等于自己的Term，则判断当前Server是否已经投票过，如果已经投票过给其他Server则拒绝本次投票，否则投票给该Candidate
3.  **(5.4.1 Election restriction)：** Candidiate如果想赢得Leader选举，则必须包含之前所有提交过的日志。即当一个Follower收到了Candidate的RequestVote请求，会判断参数中的lastLogIndex和lastLogTerm与自己的log持有的是否匹配，如果匹配才会投票

### 日志复制

当一个Candidate的投票数超过当前服务器数量的一半后，自动成为Leader，然后停止election定时器，重置appendEntries定时器，并开始定时发送AppendEntries（通过go语言的select语句实现）

**Leader:**

1. 通过for循环，只要当前状态为Leader，就不断发送首先，重置定时器
2. 检查rf.nextIndex[server]和当前日志的大小，如果要发送的server的nextIndex小于当前日志大小，则把nextIndex到后面log.length的日志都复制过来，并用RPC一起发送到目标Server中；如果要发送的Server的nextIndex等于当前日志大小，则不进行日志复制，相当于HeartBeats
3. 根据RPC返回的参数进行判断日志是否提交成功
    1. 如果reply.Term大于本身的Term，则将Leader退化为Follower状态
    2. 如果成功提交日志，则更新nextIndex和matchIndex，并判断该AppendEntries消息是否被集群中大多数（超过二分之一）的server追加日志，如果是则进行commitLog
    3. 如果没有成功提交日志，则要根据reply.Term进行判断，如果reply.Term>当前term，回退到Follower，如果reply.Term=当前term，说明preLog和当前日志不匹配，需要将nextIndex调整到返回的reply.nextIndex，并重新执行AppendEntries（这里是先设置nextIndex，等待下一次AppendEntries定时器超时才会执行，并不是立即执行一个新的RPC）

**Follower:**

Follower或者Candidate接收到AppendEntriesRPC时，首先比较RPC参数传来的term和当前的term

1. 如果当前term大于args.Term，直接返回，后面接收到的Leader回退到Follower；如果当前的小于args.Term，回退到Follower状态（防止很多Follower同时成为Candidate）继续接下来的判断
2. 先比较prevLogIndex，如果prevLogIndex不匹配，则需要设置reply.NextIndex；然后比较preLogTerm，如果Term和当前的冲突，则需要往前遍历，尝试跳过一个Term

```go
nextIndex := args.PrevLogIndex
term := rf.logs[rf.getRelativeLogIndex(args.PrevLogIndex)].Term
for nextIndex > rf.commitIndex && nextIndex > rf.lastSnapshotIndex && term == rf.logs[rf.getRelativeLogIndex(nextIndex)].Term {
   nextIndex -= 1
}
```

3. 如果preLogIndex和preLogTerm都相等，将参数中的log复制到该Follower的prevLogIndex后面
4. 日志复制完成后，更新commitIndex，并开启一个新协程来提交日志，然后返回AppendEntries成功

### 持久化Persist

每一个Raft Server都有掉线的风险，所以需要将每个raft的状态持久化到硬盘上，需要将raft的状态进行编码并保存起来

关于调用persist()和readPersist()这两个函数的时机：

- persist(): 在每次更改持久化的数据部分（currentTerm、votedFor、log[]）时需要进行调用
- readPersist(): 只有当机器挂了重启之后才需要