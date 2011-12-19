package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class READRequest extends OperationRequest {
  protected StateID mStateID;
  protected long mOffset;
  protected int mCount;
  
  @Override
  public void read(RPCBuffer buffer) {
    mStateID = new StateID();
    mStateID.read(buffer);
    mOffset = buffer.readUint64();
    mCount = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    mStateID.write(buffer);
    buffer.writeUint64(mOffset);
    buffer.writeUint32(mCount);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_READ;
  }

  public StateID getStateID() {
    return mStateID;
  }

  public void setStateID(StateID stateID) {
    this.mStateID = stateID;
  }

  public long getOffset() {
    return mOffset;
  }

  public void setOffset(long offset) {
    this.mOffset = offset;
  }

  public int getCount() {
    return mCount;
  }

  public void setCount(int count) {
    this.mCount = count;
  }
  
}
