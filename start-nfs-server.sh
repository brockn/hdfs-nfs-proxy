#!/bin/bash
set -e
if [[ $# -ne 2 ]]
then
  echo Usage: $0 HADOOP_CONF_DIR HDFS_NFS_PROXY_SNAPHOT
  echo "e.g. $0 /usr/lib/hadoop/conf snapshots/hadoop-nfs-proxy-0.80-SNAPSHOT-with-deps-0.20.2-cdh3u2.jar"
  exit 1
fi
CONFIG=$1
JAR=$2
if [[ ! -f $JAR ]]
then
  echo "Jar $JAR does not exist"
  exit 1
fi
if [[ ! -e $CONFIG ]]
then
  echo "Config dir $CONFIG does not exist"
  exit 1
fi
JAR=$(readlink -f $JAR)
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
nohup java -Xms1024m -Xmx1024m -cp $JAR:$CONFIG \
  com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Server 2051 1>nfsserver.out 2>nfsserver.err </dev/null &
pid="$!"
echo $pid > nfsserver.pid
