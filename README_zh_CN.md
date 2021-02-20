# SOFAJRaft

[![build](https://github.com/sofastack/sofa-jraft/actions/workflows/build.yml/badge.svg)](https://github.com/sofastack/sofa-jraft/actions/workflows/build.yml)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![Maven Central](https://img.shields.io/maven-central/v/com.alipay.sofa/jraft-parent.svg?label=maven%20central)](https://search.maven.org/search?q=g:com.alipay.sofa%20AND%20sofa-jraft)

SOFAJRaft 是一个基于 [RAFT](https://raft.github.io/) 一致性算法的生产级高性能 Java 实现，支持 MULTI-RAFT-GROUP，适用于高负载低延迟的场景。
使用 SOFAJRaft 你可以专注于自己的业务领域，由 SOFAJRaft 负责处理所有与 RAFT 相关的技术难题，并且 SOFAJRaft 非常易于使用，你可以通过几个示例在很短的时间内掌握它。

## 功能特性
- Leader 选举和基于优先级的半确定性 Leader 选举
- 日志复制和恢复
- 只读成员（学习者角色）
- 快照和日志压缩
- 集群线上配置变更，增加节点、删除节点、替换节点等
- 主动变更 Leader，用于重启维护，Leader 负载平衡等
- 对称网络分区容忍性
- 非对称网络分区容忍性
- 容错性，少数派故障，不影响系统整体可用性
- 多数派故障时手动恢复集群可用
- 高效的线性一致读，ReadIndex/LeaseRead
- 流水线复制
- 内置了基于 [Metrics](https://metrics.dropwizard.io/4.0.0/getting-started.html) 类库的性能指标统计，有丰富的性能统计指标
- 通过了 [Jepsen](https://github.com/jepsen-io/jepsen) 一致性验证测试
- SOFAJRaft 中包含了一个嵌入式的分布式 KV 存储实现

## 需要
编译需要 JDK 8 及以上、Maven 3.2.5 及以上。

## 文档
- [用户指南](https://www.sofastack.tech/projects/sofa-jraft/overview)
- [Counter 例子详解](https://www.sofastack.tech/projects/sofa-jraft/counter-example)
- [版本发行日志](https://www.sofastack.tech/projects/sofa-jraft/release-log)

## 如何贡献
[如何参与 SOFAJRaft 代码贡献](https://www.sofastack.tech/projects/sofa-jraft/how-to-contribute-code-to-sofajraft)

## 致谢
SOFAJRaft 是从百度的 [braft](https://github.com/brpc/braft) 移植而来，做了一些优化和改进，感谢百度 braft 团队开源了如此优秀的 C++ RAFT 实现

## 开源许可
SOFAJRaft 基于 [Apache License 2.0](./LICENSE) 协议，SOFAJRaft 依赖了一些第三方组件，它们的开源协议也为 Apache License 2.0，
另外 SOFAJRaft 也直接引用了一些开源协议为 Apache License 2.0 的代码（可能有一些小小的改动）包括：
- [JCTools](https://github.com/JCTools/JCTools) 中的 NonBlockingHashMap/NonBlockingHashMapLong
- [Netty](https://github.com/netty/netty) 中的 HashedWheelTimer，另外还参考了 Netty 的 Pipeline 设计
- [Protobuf](https://github.com/protocolbuffers/protobuf) 中对 UTF8 String 高效的编码/解码
