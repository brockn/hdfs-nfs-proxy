package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class Owner extends Attribute {
  public Owner() {
    super();
  }
  protected String mOwner;
  @Override
  public void read(RPCBuffer buffer) {
    mOwner = buffer.readString();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeString(mOwner);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_OWNER;
  }

  public String getOwner() {
    return mOwner;
  }

  public void setOwner(String owner) {
    this.mOwner = owner;
  }
}
