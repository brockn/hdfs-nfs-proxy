package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class OPENCONFIRMResponse extends OperationResponse implements Status {
  
  protected int mStatus;
  protected StateID mStateID;
  
    @Override
  public void read(RPCBuffer buffer) {
    reset();
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      mStateID = new StateID();
      mStateID.read(buffer);
    }
  }

  protected void reset() {
    mStateID = null;    
  }
  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mStatus == NFS4_OK) {
      mStateID.write(buffer);
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
    return NFS4_OP_OPEN_CONFIRM;
  }

  public StateID getStateID() {
    return mStateID;
  }

  public void setStateID(StateID stateID) {
    this.mStateID = stateID;
  }

  
}
