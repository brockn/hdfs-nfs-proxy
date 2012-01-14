#!/bin/bash
set -e
git rm snapshots/* 2>/dev/null || true
rm snapshots/* 2>/dev/null || true
BUILDS=(
  "hadoop-0.20  0.20.2-cdh3u2"
  "hadoop-0.20  1.0.0"
  "hadoop-0.23  0.23.1-SNAPSHOT"
)
TIME=$(date +%Y%m%d%H%M%S)
for build in "${BUILDS[@]}"
do
  set -- $build
  profile=$1
  version=$2
  echo $version
  test -d target/ || mkdir target/ 
  test -d snapshots/ || mkdir snapshots/
  mvn package -P$profile -Dhadoop.version=$version 1>target/build.log 2>&1 
  FILE=target/hadoop-nfs-proxy-*-SNAPSHOT.jar
  VERSION=$(echo $FILE | awk -F- '{print $4}')
  # "" would publish non-dep jars
  for name in "-with-deps"
  do
    OLD=target/hadoop-nfs-proxy-$VERSION-SNAPSHOT${name}.jar
    NEW=snapshots/hadoop-nfs-proxy-$VERSION-SNAPSHOT-${version}${name}-$TIME.jar
    mv $OLD $NEW
  done
done
