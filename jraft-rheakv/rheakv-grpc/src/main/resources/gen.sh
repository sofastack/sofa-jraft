#!/bin/bash
protoc -I=./  --java_out=../java/ rheakv_pd.proto
