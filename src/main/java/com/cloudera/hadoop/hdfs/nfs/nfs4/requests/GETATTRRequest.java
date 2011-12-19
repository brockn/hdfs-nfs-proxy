package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class GETATTRRequest extends OperationRequest {

  protected Bitmap mAttrs;
  @Override
  public void read(RPCBuffer buffer) {
    mAttrs = new Bitmap();
    mAttrs.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    mAttrs.write(buffer);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_GETATTR;
  }

  public Bitmap getAttrs() {
    return mAttrs;
  }

  public void setAttrs(Bitmap attrs) {
    this.mAttrs = attrs;
  }
  
}
