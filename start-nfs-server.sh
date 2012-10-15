#!/bin/bash
set -e
if [[ $# -ne 2 ]]
then
  echo "Usage: $0 {hadoop config dir} {port}"
  echo "e.g. $0 /usr/lib/hadoop/conf port"
  exit 1
fi
JAR=$PWD/target/hadoop-nfs-proxy-0.8-SNAPSHOT-with-deps.jar
HADOOP_CONFIG=$1
PORT=$2
if [[ ! -f $JAR ]]
then
  echo "Jar $JAR does not exist"
  exit 1
fi
if [[ ! -e $HADOOP_CONFIG ]]
then
  echo "Config dir $HADOOP_CONFIG does not exist"
  exit 1
fi
CONFIG=$PWD/conf
cd target 
if [[ -f nfsserver.pid ]]
then
  pid=`<nfsserver.pid`
  if [[ -n "$pid" ]] && kill -0 $pid 2>/dev/null
  then
    kill $pid
    sleep 1
    if kill -0 $pid 2>/dev/null
    then
      kill -9 $pid
    fi
  fi
fi
nohup java -Xmx2g -Xms2g -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -cp $CONFIG:$JAR:$HADOOP_CONFIG \
  com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Server $PORT 1>nfsserver.out 2>nfsserver.err </dev/null &
pid="$!"
echo $pid > nfsserver.pid
