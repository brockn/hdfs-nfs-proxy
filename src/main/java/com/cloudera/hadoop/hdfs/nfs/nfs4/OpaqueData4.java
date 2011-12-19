package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.Bytes;


public class OpaqueData4 extends OpaqueData {
  public OpaqueData4(long verifer) {
    this();
    this.setData(Bytes.toBytes(verifer));
  }
  public OpaqueData4() {
    super(4);
  }  
}
