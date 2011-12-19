package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class RawDevice extends Attribute  {

  protected int mMajor;
  protected int mMinor;

  @Override
  public void read(RPCBuffer buffer) {
    mMajor = buffer.readUint32();
    mMinor = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) { 
    buffer.writeUint32(mMajor);
    buffer.writeUint32(mMinor);
  }

  public int getMajor() {
    return mMajor;
  }

  public void setMajor(int major) {
    this.mMajor = major;
  }

  public int getMinor() {
    return mMinor;
  }

  public void setMinor(int minor) {
    this.mMinor = minor;
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_RAWDEV;
  }
}
