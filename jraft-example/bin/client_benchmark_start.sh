#! /bin/bash

BASE_DIR=$(dirname $0)/..
CLASSPATH=$(echo $BASE_DIR/lib/*.jar | tr ' ' ':')

# get java version
JAVA="$JAVA_HOME/bin/java"

JAVA_VERSION=$($JAVA -version 2>&1 | awk -F\" '/version/{print $2}')
echo "java version:$JAVA_VERSION path:$JAVA"

MEMORY=$(cat /proc/meminfo |grep 'MemTotal' |awk -F : '{print $2}' |awk '{print $1}' |sed 's/^[ \t]*//g')
echo -e "Total Memory:\n${MEMORY} KB\n"

if [[ $JAVA_VERSION =~ "1.8" ]]; then
  echo "Use java version 1.8 opt"

  if (($MEMORY <= 5000000));then
    JAVA_OPT_1="-server -Xms2000m -Xmx2000m -Xmn1000m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=1024m "
  elif (($MEMORY <= 9000000));then
    JAVA_OPT_1="-server -Xms3500m -Xmx3500m -Xmn1500m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=1536m "
  elif (($MEMORY <= 17000000));then
    JAVA_OPT_1="-server -Xms4g -Xmx4g -Xmn2g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=4g "
  elif (($MEMORY <= 33000000));then
    JAVA_OPT_1="-server -Xms5g -Xmx5g -Xmn2g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=8g "
  else
    JAVA_OPT_1="-server -Xms6g -Xmx6g -Xmn3g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m -Xss256k -XX:MaxDirectMemorySize=16g "
  fi

  JAVA_OPT_2="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+CMSParallelRemarkEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC"
else
  echo "Error, not support java version : $JAVA_VERSION"
  exit 1
fi

JAVA_OPTS="${JAVA_OPT_1} ${JAVA_OPT_2}"

JAVA_CONFIG=$(mktemp XXXXXXXX)
cat <<EOF | xargs echo > $JAVA_CONFIG
${JAVA_OPTS}
-cp $CLASSPATH
com.alipay.sofa.jraft.benchmark.BenchmarkBootstrap
client
$1
$2
$3
$4
$5
$6
$7
$8
EOF

JAVA_CONFIG=$(cat $JAVA_CONFIG | xargs echo)

echo $JAVA_CONFIG

JAVA="$JAVA_HOME/bin/java"

HOSTNAME=`hostname`

nohup $JAVA $JAVA_CONFIG >> client_stdout 2>&1 &
