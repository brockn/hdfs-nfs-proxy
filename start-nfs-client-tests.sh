#!/bin/bash
set -e
df -h /mnt/hdfs >/dev/null
echo "Disk Info Test Passed"
UUID=$(uuidgen)
BASE=/mnt/hdfs/user/$USER/nfs-test-$UUID
mkdir $BASE
echo "Create test passed"
java -cp target/hadoop-nfs-proxy-*-SNAPSHOT-with-deps.jar com.cloudera.hadoop.hdfs.nfs.tests.BasicTest $BASE
ls -al $BASE >/dev/null
echo "ls -la test passed."
dd if=/dev/zero of=/tmp/$USER-$UUID count=1000 2>/dev/null 1>/dev/null
mv /tmp/$USER-$UUID $BASE/large
echo "Move to hdfs passed"
mv $BASE/large $BASE/large.old
echo "Rename tests passed"
find $BASE -type f | xargs cat >/dev/null
echo "Read test passed"
find $BASE -ls >/dev/null
echo "Getattr test passed"
rm $BASE/large.old
echo "Remove test passed"
echo "Success!"
rm -rf $BASE
