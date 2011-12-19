package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public abstract class BooleanAttribute extends Attribute {

  protected boolean mValue;
  
  @Override
  public void read(RPCBuffer buffer) {
    mValue = buffer.readBoolean();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeBoolean(mValue);
  }

  public boolean get() {
    return mValue;
  }
  
  public void set(boolean value) {
    mValue = value;
  }
}
