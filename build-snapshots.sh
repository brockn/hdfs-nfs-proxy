#!/bin/bash
set -e
git rm snapshots/* 2>/dev/null || true
rm snapshots/* 2>/dev/null || true
PROFILES=(
  "cdh3"
  "hadoop020"
  "hadoop023"
  "hadoop100"
)
TIME=$(date +%Y%m%d%H%M%S)
for profile in "${PROFILES[@]}"
do
  echo $profile
  test -d target/ || mkdir target/ 
  test -d snapshots/ || mkdir snapshots/
  mvn package -P$profile 1>target/build.log 2>&1 
  FILE=target/hadoop-nfs-proxy-*-SNAPSHOT.jar
  VERSION=$(echo $FILE | awk -F- '{print $4}')
  # "" would publish non-dep jars
  for name in "-with-deps"
  do
    OLD=target/hadoop-nfs-proxy-$VERSION-SNAPSHOT${name}.jar
    NEW=snapshots/hadoop-nfs-proxy-$VERSION-SNAPSHOT-${profile}${name}-$TIME.jar
    mv $OLD $NEW
  done
done
