package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class Type extends Attribute {
  public Type() {
    super();
  }
  protected int mType;
  @Override
  public void read(RPCBuffer buffer) {
    mType = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mType);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_TYPE;
  }

  public int getType() {
    return mType;
  }

  public void setType(int type) {
    this.mType = type;
  }
}
