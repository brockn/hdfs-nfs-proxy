package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.util.Arrays;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class OpaqueData implements MessageBase {

  public OpaqueData(int size) {
    mSize = size;
    if(mSize > NFS4_OPAQUE_LIMIT) {
      throw new RuntimeException("Size too large " + mSize);
    }
  }
  protected byte[] mData;
  protected int mSize;
  @Override
  public void read(RPCBuffer buffer) {
    mData = buffer.readBytes(mSize);
  }
  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeBytes(mData, 0, mSize);
  }  
  public void setSize(int size) {
    mSize = size;
  }
  public void setData(byte[] data) {
    mData = Arrays.copyOf(data, mSize);
  }

  public int getSize() {
    return mSize;
  }
  public byte[] getData() {
    return mData;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(mData);
    result = prime * result + mSize;
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
    OpaqueData other = (OpaqueData) obj;
    if (!Arrays.equals(mData, other.mData))
      return false;
    if (mSize != other.mSize)
      return false;
    return true;
  }
}
