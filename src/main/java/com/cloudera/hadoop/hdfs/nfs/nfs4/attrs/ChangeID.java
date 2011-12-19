package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class ChangeID extends Attribute {
  public ChangeID() {
    super();
  }
  protected long mChangeID;
  @Override
  public void read(RPCBuffer buffer) {
    mChangeID = buffer.readUint64();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mChangeID);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_CHANGE;
  }

  public long getChangeID() {
    return mChangeID;
  }

  public void setChangeID(long changeID) {
    this.mChangeID = changeID;
  }
}
