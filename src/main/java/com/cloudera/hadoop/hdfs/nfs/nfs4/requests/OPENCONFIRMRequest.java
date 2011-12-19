package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class OPENCONFIRMRequest extends OperationRequest {
  protected StateID mStateID;
  protected int mSeqID;
  @Override
  public void read(RPCBuffer buffer) {
    mStateID = new StateID();
    mStateID.read(buffer); 
    mSeqID = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    mStateID.write(buffer);
    buffer.writeUint32(mSeqID);
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

  public int getSeqID() {
    return mSeqID;
  }

  public void setSeqID(int seqID) {
    this.mSeqID = seqID;
  }
  
}
