package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;

public interface RequiresCredentials {

  public AuthenticatedCredentials getCredentials();

  public void setCredentials(AuthenticatedCredentials credentials);
}
