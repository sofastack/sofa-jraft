#!/bin/bash
protoc -I=./  --java_out=../java/ rpc.proto
