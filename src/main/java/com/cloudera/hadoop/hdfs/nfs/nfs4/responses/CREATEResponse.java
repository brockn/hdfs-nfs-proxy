package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ChangeInfo;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class CREATEResponse extends OperationResponse implements Status {

  protected int mStatus;
  protected ChangeInfo mChangeInfo;
  protected Bitmap mAttrs;
  
  @Override
  public void read(RPCBuffer buffer) {
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      mChangeInfo = new ChangeInfo();
      mChangeInfo.read(buffer);
      mAttrs = new Bitmap();
      mAttrs.write(buffer);
    }
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mStatus == NFS4_OK) {
      mChangeInfo.write(buffer);
      mAttrs.write(buffer);
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
    return NFS4_OP_CREATE;
  }

  public ChangeInfo getChangeInfo() {
    return mChangeInfo;
  }

  public void setChangeInfo(ChangeInfo changeInfo) {
    this.mChangeInfo = changeInfo;
  }
  
  public void setAttrs(Bitmap attrs) {
    mAttrs = attrs;
  }
  public Bitmap getAttrs() {
    return mAttrs;
  }
}
