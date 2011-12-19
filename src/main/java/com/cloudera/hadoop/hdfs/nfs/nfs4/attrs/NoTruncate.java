package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
public class NoTruncate extends BooleanAttribute {
  public NoTruncate() {
    super();
  }
  @Override
  public int getID() {
    return NFS4_FATTR4_NO_TRUNC;
  }
  
}
