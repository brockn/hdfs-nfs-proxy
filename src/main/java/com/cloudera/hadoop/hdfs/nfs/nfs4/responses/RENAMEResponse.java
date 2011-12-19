package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.ChangeInfo;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class RENAMEResponse extends OperationResponse implements Status {

  protected int mStatus;
  protected ChangeInfo mChangeInfoSource;
  protected ChangeInfo mChangeInfoDest;
  
  @Override
  public void read(RPCBuffer buffer) {
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      mChangeInfoSource = new ChangeInfo();
      mChangeInfoSource.read(buffer);
      mChangeInfoDest = new ChangeInfo();
      mChangeInfoDest.read(buffer);
    }
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mStatus == NFS4_OK) {
      mChangeInfoSource.write(buffer);
      mChangeInfoDest.write(buffer);
    }
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
    return NFS4_OP_RENAME;
  }

  public ChangeInfo getChangeInfoSource() {
    return mChangeInfoSource;
  }

  public void setChangeInfoSource(ChangeInfo changeInfo) {
    this.mChangeInfoSource = changeInfo;
  }
  
  public ChangeInfo getChangeInfoDest() {
    return mChangeInfoDest;
  }

  public void setChangeInfoDest(ChangeInfo changeInfo) {
    this.mChangeInfoDest = changeInfo;
  }
  
}
