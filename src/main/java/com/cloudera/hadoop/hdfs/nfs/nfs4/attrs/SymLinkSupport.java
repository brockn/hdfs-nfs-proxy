package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
public class SymLinkSupport extends BooleanAttribute {
  public SymLinkSupport() {
    super();
  }
  @Override
  public int getID() {
    return NFS4_FATTR4_SYMLINK_SUPPORT;
  }  
}
