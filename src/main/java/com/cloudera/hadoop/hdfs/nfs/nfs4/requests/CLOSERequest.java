package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class CLOSERequest extends OperationRequest {
  protected int mSeqID;
  protected StateID mStateID;
  
  @Override
  public void read(RPCBuffer buffer) {
    mSeqID = buffer.readUint32();
    mStateID = new StateID();
    mStateID.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mSeqID);
    mStateID.write(buffer);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_CLOSE;
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
