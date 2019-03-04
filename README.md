# SOFAJRAFT

[![Build Status](https://travis-ci.org/alipay/sofa-jraft.svg?branch=master)](https://travis-ci.org/alipay/sofa-jraft)
![license](https://img.shields.io/badge/license-Apache--2.0-green.svg)

## Intro
An industrial-grade java implementation of RAFT consensus algorithm.
It's ported from baidu's open source project [braft](https://github.com/brpc/braft/) and optimized for java language/runtime

1. use [sofa-bolt](https://github.com/alipay/sofa-bolt) as RPC framework.
2. use [RocksDB](https://github.com/facebook/rocksdb) as log storage.
3. use [disruptor](https://github.com/LMAX-Exchange/disruptor) for batch processing.

Also we implement some features that braft missing:

1. replication pipeline optimistic.
2. readIndex for linearizable read.
3. distributed embed KV storage engine.


* 文档:
* 联系人: killme2008、fengjiachun

## Acknowledgments

Thanks braft for providing the amazing C++ implementation!

## License
[braft](https://github.com/brpc/braft/) is Apache License 2.0, jraft is the same with it.

We also reference some open source project code (may have a few modifications), including:

1. NonBlockingHashMap/NonBlockingHashMapLong in [jctool](https://github.com/JCTools/JCTools)
2. Pipeline design in [netty](https://github.com/netty/netty), HashedWheelTimer, etc.
3. The codec of utf8 string in [protobuf](https://github.com/protocolbuffers/protobuf)


