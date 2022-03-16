- Feature Name: New Snapshot Strategy
- Date: 2022-03-10
- RFC PR: #0002

## Summary

为JRaft的Snapshot生成规则，引入新的策略。

使得用户可以灵活的决定是以时间间隔‘snapshotIntervalSecs’触发快照生成，还是以日志应用间隔‘appliedIndexInterval'触发快照生成。

## Motivation

目前JRaft只支持以时间间隔‘snapshotIntervalSecs’触发快照生成，这显然是不太能满足用户需求的。

若根据时间间隔生成快照，由于各节点启动时间不同、应用RaftLog的效率不同大概率，很容易导致Raft集群中各节点的快照不一致。通常情况下，快照只是简单的作为持久化的数据备份，不太需要关注各个节点的快照是否一致。
但是，在某些特定场景，用户可能会关注各个节点所生成的快照是否是强一致的。怎么保证各个节点快照是否是强一致呢？那就得保证它们生成快照的时机是一致的，使用AppliedID间隔来触发快照生成，显然是一个不错的选择。

比起根据时间间隔‘snapshotIntervalSecs’， 基于AppliedID间隔生成快照，能更加灵活的把控RaftLog的压缩时机，将RaftLog的数量控制在一定范围内，节省磁盘空间。


## Detailed Design

引入一个枚举类型，SnapshotMode，包括： ByTimeInterval, ByIndexInterval, MixMode, None。
为NodeOptions类引入两个新配置属性，snapshotStrategy、snapshotIntervalAppliedIndex。

snapshotStrategy默认为ByTimeInterval，在默认模式下，生成快照的触发时机与之前相同，由'snapshotIntervalSecs'、'snapshotLogIndexMargin'这两个配置管控。（基于时间间隔触发）

当snapshotStrategy设置为ByIndexInterval时，生成快照的触发时间由'snapshotIntervalAppliedIndex'这个配置管控，当appliedIndex - lastSnapshotIndex >= snapshotIntervalAppliedIndex时会触发快照的生成。

当snapshotStrategy设置为MixMode时，两种生成快照的规则均会生效触发快照的生成，不建议使用此种模式。（频繁的穿插生成快照意义不大，带来不必要的资源浪费）

当snapshotStrategy设置为None时，将不再生成快照，不建议使用此种模式。（不生成快照的话，会造成RaftLog的无限膨胀，且崩溃恢复时重放RaftLog的工作量将会很大）


## Drawbacks

目前JRaft只支持以时间间隔‘snapshotIntervalSecs’触发快照生成，这显然是不太能满足用户需求的。

比起根据时间间隔‘snapshotIntervalSecs’， 基于AppliedID间隔生成快照，能更加灵活的把控RaftLog的压缩时机，将RaftLog的数量控制在一定范围内，节省磁盘空间。

## Alternative

- Why is this design the best in the space of possible designs?
- What other designs have been considered and what is the rationale for not
  choosing them?
- What is the impact of not doing this?

## Unresolved questions

What parts of the design are still to be determined?