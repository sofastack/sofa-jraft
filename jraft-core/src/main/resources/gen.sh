#!/bin/bash
protoc -I=./  --descriptor_set_out=raft.desc --java_out=../java/ enum.proto local_file_meta.proto raft.proto local_storage.proto rpc.proto cli.proto log.proto
