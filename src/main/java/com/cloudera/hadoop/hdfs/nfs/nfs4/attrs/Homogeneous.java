package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
public class Homogeneous extends BooleanAttribute {
  public Homogeneous() {
    super();
  }
  @Override
  public int getID() {
    return NFS4_FATTR4_HOMOGENEOUS;
  }
  
}
