package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class Mode extends Attribute {
  public Mode() {
    super();
  }
  protected int mMode;
  @Override
  public void read(RPCBuffer buffer) {
    mMode = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mMode);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_MODE;
  }

  public int getMode() {
    return mMode;
  }

  public void setMode(int mode) {
    this.mMode = mode;
  }
}
