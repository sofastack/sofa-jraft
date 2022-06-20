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

## 社区
- [SOFAStack 社区角色](https://github.com/sofastack/community/blob/master/ROLES.md).
- [Slack](https://join.slack.com/t/sofajraft/shared_invite/zt-1au6pb3hd-eRX_LpXPQ7r1raUu3z6wDA)

- 钉钉群
<p align="center">
<img src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*1DklS7SZFNMAAAAAAAAAAAAAARQnAQ" width="200">
</p>

- 微信公众号
<p align="center">
<img src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*LVCnR6KtEfEAAAAAAAAAAABjARQnAQ" width="222">
</p>

## 已知用户
此处列出了已知在生产环境使用了 SOFAStack 全部或者部分组件的公司或组织，大家可以通过 [SOFAStack 使用者登记](https://github.com/sofastack/sofastack.tech/issues/5)进行登记。
以下排名不分先后：
<div>
<img alt="蚂蚁集团" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*aK79TJUJykkAAAAAAAAAAAAAARQnAQ" height="60" />
<img alt="网商银行" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*uAmFRZQ0Z4YAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="恒生电子" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*_iGLRq0Ih-IAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="数立信息" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*JAgIRpjz-IgAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="Paytm" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*a0fvTKJ1Xv8AAAAAAAAAAABjARQnAQ" height="60" />
<img alt="天弘基金" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*99OQT7lDBsMAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="中国人保" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*P1BARJxwv1sAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="信美相互" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*jAzWQpIgFUAAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="南京银行" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*q9PMQI7hs8sAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="民生银行" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*GnUuSKmOtS4AAAAAAAAAAABjARQnAQ" height="60" />
<img alt="重庆农商行" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*FKrxSYhdi2wAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="中信证券" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*t-xbQb3WSjcAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="富滇银行" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*JCDYT6u6_asAAAAAAAAAAAAAARQnAQ" height="60" />
<img alt="挖财" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*lVrVT4dpSDEAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="拍拍贷" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*TAePS6j56LsAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="OPPO金融" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*mU40QaJkwZYAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="运满满" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*_kBbQYUYdIQAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="译筑科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*wuKSTpZSEkEAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="杭州米雅信息科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*b-o5SITMKu0AAAAAAAAAAABjARQnAQ" height="60" />
<img alt="邦道科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*nsw1S5bt9DkAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="申通快递" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*C3ncSpDsjS8AAAAAAAAAAABjARQnAQ" height="60" />
<img alt="深圳大头兄弟文化" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*8AYmRowxSC0AAAAAAAAAAABjARQnAQ" height="60" />
<img alt="烽火科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*MjuuT4omCrwAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="亚信科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*vBBIRomYoEUAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="成都云智天下科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*p0OkQbC5RvsAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="上海溢米辅导" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*mJdtTJsn1PwAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="态赋科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*sfLDRL5TJx8AAAAAAAAAAABjARQnAQ" height="60" />
<img alt="风一科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*EGeMR4qprnkAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="武汉易企盈" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*31WRQ7zg3HIAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="极致医疗" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*cPOiS5q8NCwAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="京东" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*INhuS44qO8YAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="小象生鲜" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*K5ERQYbCRBgAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="北京云族佳" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*qzxjSZ2tlmIAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="欣亿云网" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*huOKQKvoLzwAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="山东网聪" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*INUFR7XIH1gAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="深圳市诺安赛威" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*eVGbR7RhDDkAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="上扬软件" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*VsqMT7n7p0AAAAAAAAAAAABjARQnAQ" height="60" />
<img alt="长沙点三" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*2eEzSqdPIc0AAAAAAAAAAABjARQnAQ" height="60" />
<img alt="网易云音乐" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*66KbQ6seDqoAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="虎牙直播" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*uzr3RLUZ3RwAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="中国移动" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*vEo-T55XTOAAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="无纸科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*9aFQSLfyPhMAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="黄金钱包" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*tYZJRpANxNoAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="独木桥网络" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*GW6oTLIlAbcAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="wueasy" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*4uFWQacI-RwAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="北京攸乐科技" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*CD5VT50FXqMAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="易宝支付" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*oy0ZSquXXjAAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="威马汽车" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*LPf2TbTeJPwAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="亿通国际" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*tlq4R7QqUaEAAAAAAAAAAABkARQnAQ" height="60" />
<img alt="新华三" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*gw9uTbZvsbAAAAAAAAAAAAAAARQnAQ" height="60" />
<img alt="klilalagroup" src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*0cskToqBSi8AAAAAAAAAAAAAARQnAQ" height="60" />
</div>
