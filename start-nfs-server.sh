#!/bin/bash
set -e
CONFIG_DIR=${1?Usage: $0 Hadoop Conf Directory e.g. /usr/lib/hadoop/conf}
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
nohup java -Xms1024m -Xmx1024m -cp hadoop-nfs-proxy-1.0-SNAPSHOT-with-deps.jar:$CONFIG_DIR \
    com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Server 2049 1>nfsserver.out 2>nfsserver.err </dev/null &
pid="$!"
echo $pid > nfsserver.pid
