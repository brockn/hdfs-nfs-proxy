#!/bin/bash
set -e
java -cp target/hadoop-nfs-proxy-1.0-SNAPSHOT-with-deps.jar:$(readlink -f /usr/lib/hadoop/conf) \
    com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Server 2050 2>&1 | tee target/nfserver.out
