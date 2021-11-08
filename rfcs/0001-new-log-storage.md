- Feature Name: New logStorage
- Author: hzh 642256541@qq.com
- Date: 2021-11-8
- RFC PR: https://github.com/sofastack/sofa-jraft/pull/696

## Table of Contents

* [Summary](#Summary)
* [Motivation](#Motivation)
* [Detailed Design](#Detailed-design)
* [Drawbacks](#Drawbacks)
* [Alternative](#Alternative)
* [Unresolved questions](#Unresolved questions)

## Summary

我们希望为 `SOFAJRaft` 构建一个新的日志存储系统, 来替换原有的基于 `Rocksdb` 版本的日志系统。

## Motivation

原有的 `RocksDBLogStorage` 已经满足了日志存储的需求, 并且兼顾性能和稳定性。

但是, 使用 `SOFAJRaft` 的用户通常会使用不同版本的 `Rocksdb`, 这就要求用户不得不改变 `Rocksdb` 的版本来适应 `SOFAJRaft`, 这对于使用 `SOFAJRaft` 的用户并不用好。`

此外, `Rocksdb` 的依赖也会加大 `SOFAJRaft` 包的大小。

因此, 我们希望构建一个基于` Java` 实现的日志存储系统, 来替换原有的 `RocksDBLogStorage` 。

## Detailed Design

### System structure

下图为该日志系统的架构设计图。

其中, `DefaultLogStorage ` 为 `LogStorage ` 的实现类。

三大 `DB`  为逻辑上的存储对象, 实际的数据存储在由 `FileManager ` 所管理的 `AbstractFiles ` 中。

`AbstractFile` 封装了文件 内存映射, 读写等公共方法, 其有两个子类: `IndexFile` 和 `SegemtnFile`。

最后 `ServiceManager `  中管理的 `Service ` 起到辅助的效果, 例如 `FlushService ` 可以提供组提交的作用, `AllocateFileService` 提供文件预分配的作用。

![image-20210924210413413](https://gitee.com/zisuu/mypic4/raw/master/img/image-20210924210413413.png)

### DB

正如上图所示, 该系统需要设计三个 `DB`, 也即多了一个 `ConfDB`。

因为 `SOFAJRaft` 在启动时会将所有的 `Conf` 类型的日志加载到 `ConfigurationManager` 中, 以加速查找 `Conf` 日志。

因此, 为了方便区分  `Conf` 日志和普通日志, 在这里额外添加了一个 `ConfDB`。

当然, 为了使这三大 `DB` 能够直接复用底层的模块, 防止大量重复的代码。

因此, 我们有必要精心设计底层的 `FileManager` 和 `ServiceManager`。

### File (IndexFile / SegmentFile)

日志系统的核心肯定是文件。我们读写日志无非包含以下几个方法：

- 内存映射 mmap()
- write()
- read()
- recover()

显然， 我们需要一个公共父类， 封装这些方法， 也即该日志系统中的 `AbstractFile`

此外，`IndexFile` 和 `SegmentFile` 继承自 `AbstractFile`, 并定义了各自的日志存储方法。

**IndexFile 中存储的是固定大小的索引项， 一个索引项大小为 10 字节：**

`IndexType` 代表的是该索引项指向的日志类型（普通日志/ `conf` 类型日志)

`offset` 代表该索引项相对于该文件中存储的 第一个索引项 的偏移量

`position` 代表该索引项指向的日志 在 segmentFile 中的具体物理位置

```
 *  *    Magic byte     index type     offset        position
 *  *    [0x57]         [1 byte]      [4 bytes]     [4 bytes]
```

**SegmentFile 中存储的则是具体的日志。**

```
 *   Magic bytes     data length   data
 *   [0x57, 0x8A]    [4 bytes]     [bytes]
```

### File Management

每个 `DB` 都有属于其特有的日志文件(可能是 `IndexFile`, 也可能是 `SegmentFile`)。

因此, 我们首先需要做的事情是如何管理一个 `DB` 的所有文件。

我们可以为每个文件分配一个文件头 `FileHeader`, 其包含了文件的元信息,如:

```
FirstLogIndex  -- 该文件存储的第一个日志索引
FileFromOffset -- 该文件的起始偏移量
```

拥有以上的信息后,  `FileManager` 便可以方便的管理这些文件, 如图所示:

假设一个 `IndexFile` 大小为 126, `fileHeader` 大小为 26

![image-20211013223147858](https://gitee.com/zisuu/mypic4/raw/master/img/image-20211013223147858.png)

拥有以上的管理机制后, 我们就可以在 `FileManager `中实现一系列和文件管理相关的方法, 如:

```
findFileByLogIndex(logIndex) -- 根据日志索引查找文件
truncatePreFix/Suffix(logIndex) -- 根据日志索引缩减文件
flush(offset) -- 根据当前的偏移量进行刷盘
```

### File pre allocate -- Speed up read/write performance

作为一个高性能 `raft` 开源库,  `JRaft `需要具备低延迟, 高吞吐的日志存储能力,因此, 我们有必要加速日志存储的过程。

首先, 当大量的日志写入到本地文件时, 会存在当前日志文件已经写完, 并需要打开一块新的日志文件继续写入。

然而, 该过程涉及到内存映射技术` mmap`, 需要建立进程私有空间和文件的线性映射关系, 并且需要进行大量的缺页中断, 才能将该文件完全的加载到内存中进行读写。

这就会导致, 当我们打开一块新的文件时, 就会让日志写入性能急剧的下降, 因此, 我们有必要引入以下两点机制, 以便加速文件读写的过程:

- **文件预分配**

我们可以先预先分配空的文件, 存放在一个容器里面。当需要时, 直接从容器中取, 避免创建文件和内存映射带来的耗时过程, 使得数据写入能力能够保持原先的速度。

具体设计可以使用 '生产者 - 消费者'模式, 在 `Java` 中可以通过 `ReentrantLock + Condition `来实现:

```

// Pre-allocated files
private final ArrayDeque<AllocatedResult> blankFiles = new ArrayDeque<>();

private final Lock                        allocateLock      
private final Condition                   fullCond          
private final Condition                   emptyCond          
```

- **文件预热**

此外, 我们可以使用以下两个系统调用, 加速文件读写速度:

```
- Madvise() : 简单来说, 建议操作系统预读该文件, 操作系统可能会采纳该意见
- Mlock(): 将进程使用的部分或者全部的地址空间锁定在物理内存中，防止被操作系统回收
```

### Group commit

现在还有最后一个问题， 我们使用 `mmap` 内存映射技术，如何对写入` page cache` 的数据进行刷盘， 以持久化存储？

首先， 我们不能每写一条日志就刷盘一次， 这样会阻塞日志系统的写入速度。

可以考虑组提交的方法：

![image-20211013220524989](https://gitee.com/zisuu/mypic4/raw/master/img/image-20211013220524989.png)

流程如下：

- `LogManager` 通过调用 `appendEntries()` 批量写入日志
- `DefaultLogStorage` 通过调用` DB` 的 接口写入日志
- `DefaultLogStorage `注册一个 `FlushRequest` 到对应 `DB` 的 `FlushService` 中, 并阻塞等待. `FlushRequest ` 包含了 期望刷盘的位置 `ExpectedFlushPosition`

- `FlushService `刷到 `expectedFlushPosition` 后,  唤醒阻塞等待的 `DefaultLogStorage` 线程

通过这种组提交的方式， 一次刷盘一批日志， 可以有效的提高刷盘的性能， 减少 `IO` 次数。

## Drawbacks

> Why should we not do this?

或许一开始， 直接使用 `Rocksdb` 会免去构建索引文件这个流程， 比较方便。

## Alternative

> Why is this design the best in the space of possible designs?

该日志系统架构经由本人与 `SOFAJRaft` 开源社区负责人 `冯家纯` 前辈共同讨论， 同时借鉴了 `kafka` 消息队列的日志系统设计，并引入了

一系列的优化。

## Unresolved questions

该系统统一了日志存储和索引存储， 做到日志索引存储一体化。

但是， 在性能方面还需要增强。