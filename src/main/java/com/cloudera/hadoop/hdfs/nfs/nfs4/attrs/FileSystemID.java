package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class FileSystemID extends Attribute {
  public FileSystemID() {
    super();
  }
  protected long mMajor, mMinor;
  @Override
  public void read(RPCBuffer buffer) {
    mMajor = buffer.readUint64();
    mMinor = buffer.readUint64();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mMajor);
    buffer.writeUint64(mMinor);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_FSID;
  }

  public long getMajor() {
    return mMajor;
  }

  public void setMajor(long major) {
    this.mMajor = major;
  }

  public long getMinor() {
    return mMinor;
  }

  public void setMinor(long minor) {
    this.mMinor = minor;
  }
}
