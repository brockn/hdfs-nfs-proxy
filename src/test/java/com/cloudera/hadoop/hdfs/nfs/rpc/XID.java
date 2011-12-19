package com.cloudera.hadoop.hdfs.nfs.rpc;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class XID {
  static AtomicInteger xid = new AtomicInteger();
  static {
    xid.set((int)(System.currentTimeMillis()/1000L) + (new Random().nextInt()));
  }
  public static int next() {
    return xid.addAndGet(5);
  }
}
