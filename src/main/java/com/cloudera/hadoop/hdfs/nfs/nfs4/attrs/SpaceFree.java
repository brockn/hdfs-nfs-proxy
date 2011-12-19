package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class SpaceFree extends Attribute {
  public SpaceFree() {
    super();
  }
  protected long mValue;
  @Override
  public void read(RPCBuffer buffer) {
    mValue = buffer.readUint64();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mValue);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_SPACE_FREE;
  }

  public long get() {
    return mValue;
  }

  public void set(long value) {
    this.mValue = value;
  }
  
}
