package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
public class UniqueHandlesSupport extends BooleanAttribute {
  public UniqueHandlesSupport() {
    super();
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_UNIQUE_HANDLES;
  }  
}
