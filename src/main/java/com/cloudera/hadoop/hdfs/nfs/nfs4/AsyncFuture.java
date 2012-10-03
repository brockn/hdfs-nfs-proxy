package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.util.concurrent.Future;

public interface AsyncFuture<V>  extends Future<V> {

  /**
   * Executes the task until blocking
   * @return true if re-execution is required or false if complete
   */
  public Complete makeProgress();
  
  public static enum Complete {
    RETRY,
    COMPLETE
  };
}
