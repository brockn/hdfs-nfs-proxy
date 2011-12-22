#!/bin/bash
set -e
if [[ -f target/nfsserver.pid ]]
then
    pid=`<target/nfsserver.pid`
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
nohup java -cp target/hadoop-nfs-proxy-1.0-SNAPSHOT-with-deps.jar:$(readlink -f /usr/lib/hadoop/conf) \
    com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Server 2050 1>target/nfsserver.out 2>target/nfsserver.err </dev/null &
pid="$!"
echo $pid > target/nfsserver.pid
