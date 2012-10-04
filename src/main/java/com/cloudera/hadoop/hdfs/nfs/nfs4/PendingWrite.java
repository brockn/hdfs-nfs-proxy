package com.cloudera.hadoop.hdfs.nfs.nfs4;

public interface PendingWrite {

  public int getSize();
  public int hashCode();
  public boolean equals(Object obj);
  public String getName();
  public int getXid();
  public String getXidAsHexString();
  public long getOffset();
  public boolean isSync();
  public byte[] getData();
  public int getStart();
  public int getLength();

}