#!/bin/bash
MAVEN_REPO="/home/noland/.m2/repository"
CLASSPATH="target/classes/:target/test-classes/:$(hadoop classpath)"
CLASSPATH="${CLASSPATH}:${MAVEN_REPO}/com/google/guava/guava/r09/guava-r09.jar"
if [[ $1 == "server" ]]
then
    java -cp $CLASSPATH com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Server 2049
elif [[ $1 == "client" ]]
then
    java -cp $CLASSPATH com.cloudera.hadoop.hdfs.nfs.security.SecureClient
else
    echo "WTF $1"
    echo 1
fi
