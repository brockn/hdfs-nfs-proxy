package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class ClientID implements MessageBase {
  protected OpaqueData8 mVerifer;
  protected OpaqueData mID;
  @Override
  public void read(RPCBuffer buffer) {    
    mVerifer = new OpaqueData8();
    mVerifer.read(buffer);
    mID = new OpaqueData(buffer.readUint32());
    mID.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    mVerifer.write(buffer);
    buffer.writeUint32(mID.getSize());
    mID.write(buffer);
  }

  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }

  public OpaqueData getOpaqueID() {
    return mID;
  }

  public void setOpaqueID(OpaqueData id) {
    this.mID = id;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mID == null) ? 0 : mID.hashCode());
    result = prime * result + ((mVerifer == null) ? 0 : mVerifer.hashCode());
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
    ClientID other = (ClientID) obj;
    if (mID == null) {
      if (other.mID != null)
        return false;
    } else if (!mID.equals(other.mID))
      return false;
    if (mVerifer == null) {
      if (other.mVerifer != null)
        return false;
    } else if (!mVerifer.equals(other.mVerifer))
      return false;
    return true;
  }
}
