package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class WRITERequest extends OperationRequest {
  protected StateID mStateID;
  protected long mOffset;
  protected int mStable;
  protected byte[] mData;
  protected int mStart;
  protected int mLength;
  
  @Override
  public void read(RPCBuffer buffer) {
    mStateID = new StateID();
    mStateID.read(buffer);
    mOffset = buffer.readUint64();
    mStable = buffer.readUint32();
    mData = buffer.readBytes();
    mStart = 0;
    mLength = mData.length;
  }

  @Override
  public void write(RPCBuffer buffer) {
    mStateID.write(buffer);
    buffer.writeUint64(mOffset);
    buffer.writeUint32(mStable);
    buffer.writeUint32(mLength);
    buffer.writeBytes(mData, mStart, mLength);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_WRITE;
  }
  

  public int getStart() {
    return mStart;
  }
  public int getLength() {
    return mLength;
  }
  public byte[] getData() {
    return mData;
  }

  public StateID getStateID() {
    return mStateID;
  }

  public void setStateID(StateID stateID) {
    this.mStateID = stateID;
  }

  public void setData(byte[] data, int start, int length) {
    this.mData = data;
    this.mStart = start;
    this.mLength = length;
  }
  public long getOffset() {
    return mOffset;
  }

  public void setOffset(long offset) {
    this.mOffset = offset;
  }

  public int getStable() {
    return mStable;
  }

  public void setStable(int stable) {
    this.mStable = stable;
  }

}
