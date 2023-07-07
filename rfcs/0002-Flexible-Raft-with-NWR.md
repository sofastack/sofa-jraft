- Feature Name: Implementing Flexible Raft with NWR
- Author: yzw 1294566108@qq.com
- Date: 2023-06-28
- RFC PR: #
- Issue Link: https://github.com/sofastack/sofa-jraft/issues/1003

## Table of Contents

* [Summary](#Summary)
* [Motivation](#Motivation)
* [Key Design](#Key-design)
* [Detailed Design](#Detailed-design)

## Summary

我们希望在原始RAFT算法的基础上，让Leader选举与日志复制除了有多数派确认模型的支持之外，还可以接入NWRQuorum模型，用于动态调整一致性强度。

## Motivation

在原始的RAFT算法中，Leader选举和日志复制都需要获得多数派成员的支持。而NWR模型则可以在动态调整一致性强度的场景中使用，它需要满足W+R>N，以保证强一致性。
JRaft将RAFT和NWR结合起来，使得用户可以根据不同的业务需求来动态调整Quorum的数量。例如，在一个写多读少的场景中，用户可以将多数派的数量从3调整为2，以降低达成共识的条件，从而提高写请求的效率。同时，为了保证RAFT的正确性，写Quorum的调整需要付出代价，即读Quorum的数量也需要相应调整。
JRaft支持成员变更，因此用户可以配置(0,1]范围内的小数来计算W和R的具体值。通过使用JRaft，用户可以根据自己的业务需求来灵活地调整一致性强度，使得分布式系统在不同场景下都可以获得最佳的性能和正确性。

## Key design

下图为NWR模型Quroum的设计思路：

- 抽离出抽象父类Quorum作为MajorityQuorum（多数派确认模型，原Ballot类）和NWRQuorum（NWR模型）的模板。
- 用户首先需要在NodeOptions类中决定是否开启NWR模式，默认为多数派模型，用户手动设置读写Factor因子后，则视为开启了NWR模式。
- NodeImpl#init方法进行逻辑处理，判断是否开启了NWR模式，对读写因子进行校验与同步，然后构造对应的Quorum选票实例，例如MajorityQuorum与NWRQuorum。
- 在构建好选票实例之后，调用对应的方法可以进行选票的初始化（init）、投票（grant）等操作。

![](https://img2023.cnblogs.com/blog/2784327/202307/2784327-20230701144543736-883375762.png)

该项目涉及代码变更的地方可以划分为如下四个模块：

- **Leader选举模块：** 一个节点想要成为leader，会经过以下几个阶段：预投票、正式投票、当选leader。所以对于preVote、electSelf、becomeLeader等与多数派模型相关的方法都会涉及NWR模型的有关代码变更。
- **日志复制模块：** 当leader收到客户端的事务请求或者follower与leader数据存在差距时，会调用 **Replicator#sendEntries** 去发送日志复制消息（事务消息）；而心跳消息和探测消息，则是由 **Replicator#sendEmptyEntries** 发送的。日志复制中，**NodeImpl#executeApplyingTasks 和** **NodeImpl#unsafeApplyConfiguration** 方法会涉及到多数派确认。在执行这些方法的时候，都会使用 **BallotBox#appendPendingTask** 方法来构造一个待投票的Ballot（现在叫MajorityQuorum/NWRQuorum）并放置到投票箱中。
- **一致性读模块：** 对于一致性读模块，在raft共识算法中，读取R个节点其实体现在R个节点的心跳响应。通过R个节点的心跳，能保证这个节点一定是leader，一定拥有最新的数据，我们并不是真正需要从R个节点里面读取数据。**NodeImpl#ReadIndexHeartbeatResponseClosure** 这样的方法，可以看到执行了心跳消息的多数派确认模型的逻辑，ReadIndexHeartbeatResponseClosure构造器里面传入了quorum的值，这里我们需要对应修改为NWR模型的逻辑。
- **成员变更模块：** 对于JRaft成员变更来讲，核心逻辑是采用单成员变更的方式，即使需要同时变更多个成员时，也是会先整理出新add与新remove的成员，再逐个进行单成员变更。其核心方法 **addPeer、removePeer、changePeers、resetPeers** 等等都会涉及NWR模型的适配。另外对于stepDownTimer计时器，它会处理那些下线的节点，对于stepDown而言的多数派逻辑也需要修改。

![](https://img2023.cnblogs.com/blog/2784327/202307/2784327-20230701144554871-1252032815.png)
## Detailed Design

### NodeOptions

在**NodeOptions类**中，我们新增了如下三个参数：readQuorumFactor、writeQuorumFactor与enableFlexibleRaft，分别表示读因子、写因子以及是否开启NWR模型（true），默认不开启，表示多数派确认模型（false）

```
    /**
     * Read Quorum's factor
     */
    private Integer                          readQuorumFactor;
    /**
     * Write Quorum's factor
     */
    private Integer                          writeQuorumFactor;
    /**
     * Enable NWRMode or Not
     */
    private boolean                         enableFlexibleRaft          = false;
```

对于readQuorumFactor和writeQuorumFactor两个属性，在NodeOptions类里提供了setter和getter方法便于用户自定义配置。对于enableFlexibleRaft属性，提供了isEnableFlexibleRaft()来判断是否开启NWR模型，而enableFlexibleRaft()方法表示开启NWR模式。

```
    public Integer getReadQuorumFactor() {
        return readQuorumFactor;
    }

    public void setReadQuorumFactor(int readQuorumFactor) {
        this.readQuorumFactor = readQuorumFactor;
        enableFlexibleRaft();
    }

    public Integer getWriteQuorumFactor() {
        return writeQuorumFactor;
    }

    public void setWriteQuorumFactor(int writeQuorumFactor) {
        this.writeQuorumFactor = writeQuorumFactor;
        enableFlexibleRaft();
    }

    public boolean isEnableFlexibleRaft() {
        return enableFlexibleRaft;
    }

    private void enableFlexibleRaft() {
        this.enableFlexibleRaft = true;
    }
```

### Node Init

在**NodeImpl#init**时，我们首先会对NodeOptions内的readFactor和writeFactor进行校验并且进行参数同步，如果用户只设置了readFactor和writeFactor两个参数的其中之一，那么我们需要同步这两个参数的值。
在init方法初始化node时，会首先对NWR模式下的factor进行校验与同步。

```
if(options.isEnableFlexibleRaft() && !checkAndResetFactor(options.getWriteQuorumFactor(),
         options.getReadQuorumFactor())){
     return false;
}
```

校验与同步逻辑在方法checkAndResetFactor里：

```
    private boolean checkAndResetFactor(Integer writeFactor, Integer readFactor){
        if (Objects.nonNull(readFactor) && Objects.nonNull(writeFactor)) {
            if (readFactor + writeFactor != 10) {
                LOG.error("The sum of readFactor and writeFactor should be 10");
                return false;
            }
            return true;
        }
        if (Objects.nonNull(readFactor)) {
            if (readFactor > 0 && readFactor < 10) {
                options.setWriteQuorumFactor(10 - readFactor);
                return true;
            }
            LOG.error("Fail to set quorum_nwr read_factor because {} is not between (0,10)", readFactor);
        }
        if (Objects.nonNull(writeFactor)) {
            if (writeFactor > 0 && writeFactor < 10) {
                options.setReadQuorumFactor(10 - writeFactor);
                return true;
            }
            LOG.error("Fail to set quorum_nwr write_factor because {} is not between (0,10)", writeFactor);
        }
        return false;
    }
```

在以往多数派确认模型中，node初始化时生成Ballot对象是通过关键字直接new出来的，如下所示：

```
private final Ballot voteCtx = new Ballot(); 
private final Ballot prevVoteCtx = new Ballot();
```

添加NWR模型后，我们需要判断，到底是生成MajorityQuorum还是NWRQuorum。所以在对节点进行初始化时（NodeImpl#init），会根据NodeOptions判断是否开启NWR模型，进而构造对应实例。

```
prevVoteCtx = options.isEnableFlexibleRaft() ? new NWRQuorum(opts.getReadQuorumFactor(), opts.getWriteQuorumFactor())
    : new MajorityQuorum();
voteCtx = options.isEnableFlexibleRaft() ? new NWRQuorum(opts.getReadQuorumFactor(), opts.getWriteQuorumFactor())
    : new MajorityQuorum();
```

### Quorum Detail

#### Quorum

Quorum作为NWRQuorum与MajorityQuorum的抽象父类，持有peers、oldPeers、quorum、oldQuorum几个公共属性。

```
protected final List<Quorum.UnfoundPeerId> peers = new ArrayList<>()
protected int quorum;
protected final List<Quorum.UnfoundPeerId> oldPeers = new ArrayList<>();
protected int oldQuorum;
```

Quorum提供了grant和init两个抽象方法，子类实现该抽象方法的具体业务逻辑。

```
public abstract boolean init(final Configuration conf, final Configuration oldConf);
public abstract void grant(final PeerId peerId);
```

Quorum还定义了findPeer、isGranted、grant这三个包含方法体的父类方法。

```
public PosHint grant(final PeerId peerId, final PosHint hint){
    //此处省略方法体
}
public boolean isGranted() {
    //此处省略方法体
}
private UnfoundPeerId findPeer(final PeerId peerId, final List<UnfoundPeerId> peers, final int posHint){
    //此处省略方法体
}
```

#### NWRQuorum

NWRQuorum作为NWR模型选票实现类，持有readFactor、writeFactor等几个属性，他们代表读写因子。

```
    protected Integer readFactor; ---读因子
    protected Integer writeFactor; ---写因子
    private static final String defaultDecimalFactor = "0.1";
    private static final BigDecimal defaultDecimal = new BigDecimal(defaultDecimalFactor);
```

另外，我们提供了一个NWRQuorum的构造器用于构造NWRQuorum实例，需要传入writeFactor, readFactor两个参数。

```
    public NWRQuorum(Double writeFactor, Double readFactor) {
        this.writeFactor = writeFactor;
        this.readFactor = readFactor;
    }
```

我们也实现了抽象父类的init与grant方法，

```
public boolean init(Configuration conf, Configuration oldConf) ---初始化选票
public void grant(final PeerId peerId) ---节点投票
```

对于NWRQuorum的init()方法来讲，他对于quorum的计算与以往有所不同，代码如下：

```
    @Override
    public boolean init(Configuration conf, Configuration oldConf) {
        peers.clear();
        oldPeers.clear();
        quorum = oldQuorum = 0;
        int index = 0;

        if (conf != null) {
            for (final PeerId peer : conf) {
                peers.add(new UnfoundPeerId(peer, index++, false));
            }
        }

        BigDecimal writeFactorDecimal = defaultDecimal.multiply(new BigDecimal(writeFactor))
                .multiply(new BigDecimal(peers.size()));
        quorum = writeFactorDecimal.setScale(0, RoundingMode.CEILING).intValue();

        if (oldConf == null) {
            return true;
        }
        index = 0;
        for (final PeerId peer : oldConf) {
            oldPeers.add(new UnfoundPeerId(peer, index++, false));
        }

        BigDecimal writeFactorOldDecimal = defaultDecimal.multiply(new BigDecimal(writeFactor))
                .multiply(new BigDecimal(oldPeers.size()));
        oldQuorum = writeFactorOldDecimal.setScale(0, RoundingMode.CEILING).intValue();
        return true;
    }
```

#### MajorityQuorum

MajorityQuorum实现了Quorum抽象父类的两个方法，init方法初始化选票需要参数，grant方法用于投票。

```
public boolean init(final Configuration conf, final Configuration oldConf) ---初始化选票 
public void grant(final PeerId peerId) ---节点投票
```

### Module Detail

#### **Leader-election Module**

一个节点成为leader会经过以下几个阶段：预投票、正式投票、当选leader。
首先我们来看预投票NodeImpl#preVote()方法，大概经历以下几个过程：

1. 校验是否可以开启预投票，安装快照或者集群配置不包含本节点都不可以开启预投票。
2. 初始化预投票-投票箱。
3. 遍历，给除了本节点之外的所有其他节点发起RequestVoteRequest--RPC请求。
4. 给自己投票，并判断是否已经达到多数派。


其中**预投票**有以下几处核心代码涉及投票：

- **在NodeImpl#preVote()中**，调用Quorum#init()方法初始化预投票-投票箱

```
prevVoteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
```

- **在NodeImpl#preVote()中**，本节点自己投票，在判断投票箱达到多数派后开启正式选举，调用electSelf()

```
            prevVoteCtx.grant(this.serverId);
            if (prevVoteCtx.isGranted()) {
                doUnlock = false;
                electSelf();
            }
```

- **在NodeImpl#handlePreVoteResponse中**，该方法用来处理预投票响应：首先根据响应判断对方节点是否为本节点投票，在判断为true后，Quorum(即prevVoteCtx)调用grant()对该节点进行授权投票。最后通过isGranted()判断是否大多数节点已经确认，如果符合条件，则开启正式选举模式，调用electSelf()方法。

```
            // check granted quorum?
            if (response.getGranted()) {
                prevVoteCtx.grant(peerId);
                if (prevVoteCtx.isGranted()) {
                    doUnlock = false;
                    electSelf();
                }
            }
```

- 接下来多数派确认后，执行NodeImpl#electSelf()方法，它做了以下几件事：

1. 检验当前节点是否存在集群配置里面，不存在不进行选举。
2. 关闭预选举定时器。
3. 清空leader，增加任期，修改状态为candidate，votedId设置为当前本节点。
4. 启动投票定时器voteTimer，因为可能投票失败需要循环发起投票，voteTimer里面会根据当前的CANDIDATE状态调用electSelf进行选举。
5. 初始化投票箱。
6. 遍历所有节点，向其他集群节点发送RequestVoteRequest--RPC请求，请求被RequestVoteRequestProcessor处理器处理的。
7. 如果多数派确认，则调用NodeImpl#becomeLeader晋升为leader。

```
            voteCtx.grant(this.serverId);
            if (voteCtx.isGranted()) {
                becomeLeader();
            }
```

- **在NodeImpl#handleRequestVoteResponse中**，该方法用来处理投票请求的响应。只要收到投票的反馈，就会在投票箱中对多数派进行确认，如果已经达成多数派确认的共识，那么本节点就调用NodeImpl#becomeLeader方法成为leader。投票请求处理器NodeImpl#handleRequestVoteResponse方法对选票处理的核心逻辑如下：

```
            // check granted quorum?
            if (response.getGranted()) {
                voteCtx.grant(peerId);
                if (voteCtx.isGranted()) {
                    becomeLeader();
                }
            }
```

- 在多数派确认后，会调用NodeImpl#becomeLeader方法正式被选举为leader：

1. 首先会停止选举定时器。
2. 设置当前的状态为leader。
3. 设值任期。
4. 遍历所有的节点将节点加入到复制集群中。
5. 最后将stepDownTimer打开，定时对leader进行校验是不是又半数以上的节点响应当前的leader。

#### Log-replication Module

当leader收到客户端的事务请求或者follower与leader数据存在差距时，会调用**Replicator#sendEntries**去复制日志，日志复制消息属于事务消息；而心跳消息和探测消息，则是由**Replicator#sendEmptyEntries**发送的。
日志复制的流程如下：

1. leader将日志项追加到本地日志
2. leader将日志广播给follower
3. follower追加到本地日志
4. follower返回执行结果
5. leader收到多数派响应后提交日志
6. 返回执行结果给客户端
   在JRaft中，我阅读了日志复制模块的源码部分，然后总结出下图来直观的反应整个日志复制从leader到follower再回到leader的整个过程，详细的方法调用链路过程如下所示：

![](https://img2023.cnblogs.com/blog/2784327/202307/2784327-20230701144642277-1586182824.png)
在日志复制中，以下方法会涉及到多数派确认：NodeImpl#executeApplyingTasks 和NodeImpl#unsafeApplyConfiguration，也就是执行应用任务和应用配置变更所使用到的日志复制。在执行这些方法的时候，都会使用BallotBox#appendPendingTask方法来构造一个待投票的Quorum并放置到投票箱中。

**场景一：应用任务**

我们首先分析NodeImpl#executeApplyingTasks方法：

1. 检查当前节点是否是 Leader 节点。如果节点不是 Leader 节点，则将所有任务的状态设置为错误并执行相应的回调方法；如果节点正在进行领导权转移，则将所有任务的状态设置为繁忙并执行相应的回调方法。
2. 遍历任务列表，对于每个任务执行以下操作：a. 检查任务的 expectedTerm 是否与当前任期相同，如果不同则将任务的状态设置为错误并执行相应的回调方法。b. 将任务添加到 BallotBox 中。c. 将任务的日志条目信息添加到一个列表中，并将任务重置为默认状态。
3. 将任务列表中的所有日志条目追加到当前节点的日志中，并将追加操作封装为 LeaderStableClosure 回调方法。
4. 检查并更新配置信息，如果需要更新则执行相应的更新操作。

**注意：在executeApplyingTasks方法中，根据当前节点配置，生成了一个待投票Quorum，并放置到投票箱BallotBox的pendingMetaQueue中。所以我们需要在这里构造待投票Quorum时，修改quorum为NWR模式，而不是之前的多数派。**

```
    private void executeApplyingTasks(final List<LogEntryAndClosure> tasks)  {
        // 省略部分代码...
       if (!this.ballotBox.appendPendingTask(this.conf.getConf(),
          this.conf.isStable() ? null : this.conf.getOldConf(), task.done,options.isEnableFlexibleRaft() ?
          QuorumFactory.createNWRQuorumConfiguration(options.getWriteQuorumFactor(), options.getReadQuorumFactor()):
          QuorumFactory.createMajorityQuorumConfiguration())) {
          ThreadPoolsFactory.runClosureInThread(this.groupId, task.done, new Status(RaftError.EINTERNAL, "Fail to append task."));
          task.reset();
          continue;
          }
        // 省略部分代码...
        this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
        // 省略部分代码...
    }
```

**场景二：应用配置**

接下来看看NodeImpl#unsafeApplyConfiguration是如何构建选票Ballot的：

下面这段代码的作用是将新配置信息封装成一个日志条目，并追加到当前节点的日志中，从而实现配置变更的操作。其实逻辑和增加普通日志类似，主要需要注意的还是ballotBox.appendPendingTask方法，也就是生成一个待投票Quorum的逻辑。

```
    private void unsafeApplyConfiguration(final Configuration newConf, final Configuration oldConf,
                                          final boolean leaderStart) {
        // 省略部分代码...
        if (!this.ballotBox.appendPendingTask(newConf, oldConf, configurationChangeDone,options.isEnableFlexibleRaft() ?
            QuorumFactory.createNWRQuorumConfiguration(options.getWriteQuorumFactor(), options.getReadQuorumFactor()):
                QuorumFactory.createMajorityQuorumConfiguration())) {
            ThreadPoolsFactory.runClosureInThread(this.groupId, configurationChangeDone, new Status(
                RaftError.EINTERNAL, "Fail to append task."));
            return;
        }
        // 省略部分代码...
        this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
        checkAndSetConfiguration(false);
    }
```

##### QuorumFactory

在上面的**executeApplyingTasks与unsafeApplyConfiguration**方法中使用到了QuorumFactory这个工厂类的方法。为了更方便配置一个Quorum的属性，可以将factor因子和NWR开关整合到QuorumConfiguration类中，以便于快速构建一个QuorumConfiguration。实现代码如下：

```
public final class QuorumFactory {
    public static QuorumConfiguration createNWRQuorumConfiguration(Integer writeFactor,Integer readFactor) {
        boolean isEnableNWR = true;
        QuorumConfiguration quorumConfiguration = new QuorumConfiguration();
        quorumConfiguration.setReadFactor(readFactor);
        quorumConfiguration.setWriteFactor(writeFactor);
        quorumConfiguration.setEnableNWR(isEnableNWR);
        return quorumConfiguration;
    }

    public static QuorumConfiguration createMajorityQuorumConfiguration(){
        boolean isEnableNWR = false;
        QuorumConfiguration quorumConfiguration = new QuorumConfiguration();
        quorumConfiguration.setEnableNWR(isEnableNWR);
        return quorumConfiguration;
    }
}
```

对于BallotBox#CommitAt来说，在进行确认时，只需要从pendingMetaQueue获取Quorum再进行grant授权投票即可，之后再判断是否已经达到（多数派/NWR）确认。

```
    public boolean commitAt(final long firstLogIndex, final long lastLogIndex, final PeerId peer) {
            // 省略部分代码...
            Quorum.PosHint hint = new Quorum.PosHint();
            for (long logIndex = startAt; logIndex <= lastLogIndex; logIndex++) {
                final Quorum quorum = this.pendingMetaQueue.get((int) (logIndex - this.pendingIndex));
                hint = quorum.grant(peer, hint);
                if (quorum.isGranted()) {
                    lastCommittedIndex = logIndex;
                }
            }
            // 省略部分代码...
        this.waiter.onCommitted(lastCommittedIndex);
        return true;
    }
```

#### Consistent-reading Module

对于**ReadIndexHeartbeatResponseClosure**类来讲，他的run方法执行了心跳消息多数派确认逻辑。其构造器内传入的quorum值需要进行NWR模型适配，并且failPeersThreshold属性也需要重新适配计算逻辑。
原有获取ReadQuorum数值的多数派确认逻辑是：

```
    private int getQuorum() {
        final Configuration c = this.conf.getConf();
        if (c.isEmpty()) {
            return 0;
        }
        return c.getPeers().size() / 2 + 1;
    }
```

如今我们需要修改该方法，额外对NWR模型进行判断：

```
    private int getReadQuorum() {
        final Configuration c = this.conf.getConf();
        if (c.isEmpty()) {
            return 0;
        }
        int size = c.getPeers().size();
        if(!options.isEnableFlexibleRaft()){
            return size / 2 + 1;
        }
        int writeQuorum = new BigDecimal("0.1").multiply(new BigDecimal(options.getWriteQuorumFactor()))
                .multiply(new BigDecimal(c.getPeers().size())).setScale(0, RoundingMode.CEILING).intValue();
        return size - writeQuorum + 1;
    }
```

failPeersThreshold原有计算逻辑：

```
this.failPeersThreshold = peersCount % 2 == 0 ? (quorum - 1) : quorum;
```

修改后：

```
this.failPeersThreshold = options.isEnableFlexibleRaft() ? peersCount - quorum + 1 :
     (peersCount % 2 == 0 ? (quorum - 1) : quorum);
```
#### Member change

##### addPeer

新增成员的逻辑是：基于原conf生成一份新conf，并添加指定peer节点，然后调用unsafeRegisterConfChange方法执行成员配置变更，并且为当前成员增加Replicator对象进行日志复制。对于新增成员来说，需要给他一段学习时间，赶上leader的日志进度，然后才加入集群。

##### removePeer

移除成员的逻辑是：基于原conf生成一份新conf，这份新配置移除了指定的peer，然后调用unsafeRegisterConfChange方法执行成员配置变更，对于移除成员，直接调用nextStage()进入下一阶段记录成员变更日志项。

##### changePeers

与前两者方法类似。不过对于处理多成员变更，也是需要进行逐个成员变更的，也就是所谓的单节点变更。

##### resetPeers

这个方法用于强制变更本节点的配置，单独重置该节点的配置，而在该节点成为领导者之前，无需复制其他同行。 当复制组的大多数已死时，应该调用此功能。在这种情况下，一致性和共识都不能保证，在处理此方法时要小心。

##### stepDown

另外，stepDownTimer计时器会处理那些下线的节点。当一个集群中，下线节点数量超过多数派数量时，将会导致整个集群不可用，在**NodeImpl#checkDeadNodes0**方法中，会校验已经死亡的节点，其中涉及到的多数派模型代码如下：


```
        if (aliveCount >= peers.size() / 2 + 1) {
            updateLastLeaderTimestamp(startLease);
            return true;
        }
```

由于加入NWR模型，我们需要修改为

```
        if (aliveCount >= getReadQuorum()) {
            updateLastLeaderTimestamp(startLease);
            return true;
        }
```

另外对于checkDeadNodes方法来讲，如果当下线节点数量不再满足MajorityQuorum或者ReadQuorum时，将会报错并且将leader节点stepDown。

```
        if (stepDownOnCheckFail) {
            LOG.warn("Node {} steps down when alive nodes don't satisfy quorum, term={}, deadNodes={}, conf={}.",
                getNodeId(), this.currTerm, deadNodes, conf);
            final Status status = new Status();
            String msg = options.isEnableFlexibleRaft() ? "Reading quorum does not meet availability conditions: "
                    + getReadQuorum() + ", Some nodes in the cluster dies" :
                    "Majority of the group dies";
            status.setError(RaftError.ERAFTTIMEDOUT, "%s: %d/%d", msg,
                    deadNodes.size(), peers.size());
            stepDown(this.currTerm, false, status);
        }
```

1