package com.cloudera.hadoop.hdfs.nfs.nfs4;

/**
 * Return the exact size of the object when written out in byte form.
 */
public interface WireSize extends MessageBase {
  public int getWireSize();
}
