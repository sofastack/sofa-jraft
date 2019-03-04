#!/bin/sh

echo "Prepare to kill benchmark process"

pid=`ps ax | grep -i 'com.alipay.sofa.jraft.benchmark.BenchmarkBootstrap' | grep java | awk '{print $1}'`
if [ ! -n "$pid" ] ; then
   echo "$LOG_PREFIX no java process running"
else
   kill $pid
   echo $LOG_PREFIX kill $pid
fi

if [ ! -n "$pid" ] ; then
	echo "$LOG_PREFIX kill successfully"
else
    kill -9 $pid
    sleep 1
    echo "$LOG_PREFIX kill process $pid successfully"
fi