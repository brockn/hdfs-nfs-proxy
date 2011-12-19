package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
public class ACLSupport extends BooleanAttribute {
  public ACLSupport() {
    super();
  }
  @Override
  public int getID() {
    return NFS4_FATTR4_ACL_SUPPORT;
  }
  
}
