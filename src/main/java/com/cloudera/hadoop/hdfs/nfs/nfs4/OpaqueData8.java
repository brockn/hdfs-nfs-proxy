package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.Bytes;


public class OpaqueData8 extends OpaqueData {
  public OpaqueData8(long verifer) {
    this();
    this.setData(Bytes.toBytes(verifer));
  }
  public OpaqueData8() {
    super(8);
  }  
}
