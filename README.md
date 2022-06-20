# SOFAJRaft

[![build](https://github.com/sofastack/sofa-jraft/actions/workflows/build.yml/badge.svg)](https://github.com/sofastack/sofa-jraft/actions/workflows/build.yml)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![Maven Central](https://img.shields.io/maven-central/v/com.alipay.sofa/jraft-parent.svg?label=maven%20central)](https://search.maven.org/search?q=g:com.alipay.sofa%20AND%20sofa-jraft)

[中文](README_zh_CN.md)
## Overview
SOFAJRaft is a production-level, high-performance Java implementation based on the [RAFT](https://raft.github.io/) consistency algorithm that supports MULTI-RAFT-GROUP for high-load, low-latency scenarios.
With SOFAJRaft you can focus on your business area. SOFAJRaft handles all RAFT-related technical challenges. SOFAJRaft is very user-friendly, which provides several examples, making it easy to understand and use.

## Features
- Leader election and priority-based semi-deterministic leader election
- Log replication and recovery
- Read-only member (learner)
- Snapshot and log compaction
- Cluster membership management, adding nodes, removing nodes, replacing nodes, etc.
- Mechanism of transfer leader for reboot, load balance scene, etc.
- Symmetric network partition tolerance
- Asymmetric network partition tolerance
- Fault tolerance, minority failure doesn't affect the overall availability of system
- Manual recovery cluster available for majority failure
- Linearizable read, ReadIndex/LeaseRead
- Replication pipeline
- Rich statistics to analyze the performance based on [Metrics](https://metrics.dropwizard.io/4.0.0/getting-started.html)
- Passed [Jepsen](https://github.com/jepsen-io/jepsen) consistency verification test
- SOFAJRaft includes an embedded distributed KV storage implementation

## Requirements
Compile requirement: JDK 8+ and Maven 3.2.5+ .

## Documents
- [User Guide](https://www.sofastack.tech/projects/sofa-jraft/overview)
- [Counter Example Details](https://www.sofastack.tech/projects/sofa-jraft/counter-example)
- [Release Notes](https://www.sofastack.tech/projects/sofa-jraft/release-log)

## Contribution
[How to contribute](https://www.sofastack.tech/projects/sofa-jraft/how-to-contribute-code-to-sofajraft)

## Acknowledgement
SOFAJRaft was ported from Baidu's [braft](https://github.com/brpc/braft) with some optimizing and improvement. Thanks to the Baidu braft team for opening up such a great C++ RAFT implementation.

## License
SOFAJRaft is licensed under the [Apache License 2.0](./LICENSE). SOFAJRaft relies on some third-party components, and their open source protocol is also Apache License 2.0.
In addition, SOFAJRaft also directly references some code (possibly with minor changes), which open source protocol is Apache License 2.0, including
- NonBlockingHashMap/NonBlockingHashMapLong in [JCTools](https://github.com/JCTools/JCTools)
- HashedWheelTimer in [Netty](https://github.com/netty/netty), also referenced Netty's Pipeline design
- Efficient encoding/decoding of UTF8 String in [Protobuf](https://github.com/protocolbuffers/protobuf)

## Community
See our community [materials](https://github.com/sofastack/community/blob/master/ROLES-EN.md).

Join the user group on [Slack](https://join.slack.com/t/sofajraft/shared_invite/zt-1au6pb3hd-eRX_LpXPQ7r1raUu3z6wDA)

Scan the QR code below with DingTalk(钉钉) to join the SOFAStack user group.
<p align="center">
<img src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*1DklS7SZFNMAAAAAAAAAAAAAARQnAQ" width="200">
</p>

Scan the QR code below with WeChat(微信) to Follow our Official Accounts.
<p align="center">
<img src="https://gw.alipayobjects.com/mdn/sofastack/afts/img/A*LVCnR6KtEfEAAAAAAAAAAABjARQnAQ" width="222">
</p>

## Known Users
These are the companies using SOFAStack (the names are in no particular order). Please leave a comment [here](https://github.com/sofastack/sofastack.tech/issues/5) to tell us your scenario to make SOFAStack better.
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
