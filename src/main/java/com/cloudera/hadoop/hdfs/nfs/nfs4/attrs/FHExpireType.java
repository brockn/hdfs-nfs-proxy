package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class FHExpireType extends Attribute {
  public FHExpireType() {
    super();
  }
  protected int mExpireType;
  @Override
  public void read(RPCBuffer buffer) {
    mExpireType = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mExpireType);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_FH_EXPIRE_TYPE;
  }

  public int getExpireType() {
    return mExpireType;
  }

  public void setExpireType(int expireType) {
    this.mExpireType = expireType;
  }
  
}
