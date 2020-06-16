#!/bin/bash
protoc -I=./  --java_out=../java/ counter.proto
