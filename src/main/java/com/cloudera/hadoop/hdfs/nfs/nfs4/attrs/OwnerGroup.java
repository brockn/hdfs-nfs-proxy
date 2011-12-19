package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class OwnerGroup extends Attribute {
  public OwnerGroup() {
    super();
  }
  protected String mOwnerGroup;
  @Override
  public void read(RPCBuffer buffer) {
    mOwnerGroup = buffer.readString();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeString(mOwnerGroup);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_OWNER_GROUP;
  }

  public String getOwnerGroup() {
    return mOwnerGroup;
  }

  public void setOwnerGroup(String ownerGroup) {
    this.mOwnerGroup = ownerGroup;
  }
}
