package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
public class ChownRestricted extends BooleanAttribute {
  public ChownRestricted() {
    super();
  }
  @Override
  public int getID() {
    return NFS4_FATTR4_CHOWN_RESTRICTED;
  }
  
}
