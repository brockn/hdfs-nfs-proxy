package com.cloudera.hadoop.hdfs.nfs.security;

public interface AuthenticatedCredentials {
  public int getUID();
  public int getGID();

}
