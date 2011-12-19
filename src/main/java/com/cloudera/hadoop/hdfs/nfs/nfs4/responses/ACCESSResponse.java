package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class ACCESSResponse extends OperationResponse implements Status {

  protected int mStatus;
  protected int mSupported;
  protected int mAccess;
  
  @Override
  public void read(RPCBuffer buffer) {
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      mSupported = buffer.readUint32();
      mAccess = buffer.readUint32();
    }
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mStatus == NFS4_OK) {
      buffer.writeUint32(mSupported);
      buffer.writeUint32(mAccess);
    }
  }
  public int getAccess() {
    return mAccess;
  }

  public void setAccess(int access) {
    this.mAccess = access;
  }
  
  public int getSupported() {
    return mSupported;
  }

  public void setSupported(int supported) {
    this.mSupported = supported;
  }

  @Override
  public int getStatus() {
    return mStatus;
  }
  @Override
  public void setStatus(int status) {
    this.mStatus = status;
  }
  @Override
  public int getID() {
    return NFS4_OP_ACCESS;
  }
}
