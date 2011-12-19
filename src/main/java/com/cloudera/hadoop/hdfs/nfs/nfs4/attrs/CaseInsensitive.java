package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
public class CaseInsensitive extends BooleanAttribute {
  public CaseInsensitive() {
    super();
  }
  @Override
  public int getID() {
    return NFS4_FATTR4_CASE_INSENSITIVE;
  }
  
}
