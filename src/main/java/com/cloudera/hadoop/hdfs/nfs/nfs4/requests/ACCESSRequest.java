package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class ACCESSRequest extends OperationRequest {
  protected int mAccess;
  @Override
  public void read(RPCBuffer buffer) {
    mAccess = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mAccess);
  }

  @Override
  public int getID() {
    return NFS4_OP_ACCESS;
  }

  public int getAccess() {
    return mAccess;
  }

  public void setAccess(int access) {
    this.mAccess = access;
  }
  
  
}
