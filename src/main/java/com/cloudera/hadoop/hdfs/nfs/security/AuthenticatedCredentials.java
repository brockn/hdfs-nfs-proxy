package com.cloudera.hadoop.hdfs.nfs.security;

/**
 * Credentials which have an identifier associated with them
 * should implement this interface. At present that is only
 * system credentials and the interface might be changed
 * when kerberos is included.
 */
public interface AuthenticatedCredentials {
  public int getUID();
  public int getGID();
}
