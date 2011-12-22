package com.cloudera.hadoop.hdfs.nfs.nfs4;

/**
 * Requests and attributes in NFS have an integer identifier.
 */
public interface Identifiable {

  public int getID();
}
