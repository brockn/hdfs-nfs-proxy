package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class Callback implements MessageBase {
  protected int mCallbackProgram;
  protected String mNetID;
  protected String mAddr;
  
  @Override
  public void read(RPCBuffer buffer) {
    mCallbackProgram = buffer.readUint32();
    mNetID = buffer.readString();
    mAddr = buffer.readString();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mCallbackProgram);
    buffer.writeString(mNetID);
    buffer.writeString(mAddr);
  }
  
  

  public int getCallbackProgram() {
    return mCallbackProgram;
  }

  public void setCallbackProgram(int mCallbackProgram) {
    this.mCallbackProgram = mCallbackProgram;
  }

  public String getNetID() {
    return mNetID;
  }

  public void setNetID(String mNetID) {
    this.mNetID = mNetID;
  }

  public String getAddr() {
    return mAddr;
  }

  public void setAddr(String mAddr) {
    this.mAddr = mAddr;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mAddr == null) ? 0 : mAddr.hashCode());
    result = prime * result + mCallbackProgram;
    result = prime * result + ((mNetID == null) ? 0 : mNetID.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Callback other = (Callback) obj;
    if (mAddr == null) {
      if (other.mAddr != null)
        return false;
    } else if (!mAddr.equals(other.mAddr))
      return false;
    if (mCallbackProgram != other.mCallbackProgram)
      return false;
    if (mNetID == null) {
      if (other.mNetID != null)
        return false;
    } else if (!mNetID.equals(other.mNetID))
      return false;
    return true;
  }
}
