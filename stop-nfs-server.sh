#!/bin/bash
set -e
cd target
if [[ -f nfsserver.pid ]]
then
  pid=`<nfsserver.pid`
  if kill -0 $pid 2>/dev/null
  then
    kill $pid
  fi
  sleep 2
  if kill -0 $pid 2>/dev/null
  then
    kill -9 $pid
  fi
fi
