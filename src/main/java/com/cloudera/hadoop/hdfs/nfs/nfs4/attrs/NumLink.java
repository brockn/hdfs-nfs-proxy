package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class NumLink extends Attribute {
  public NumLink() {
    super();
  }
  protected int mValue;
  @Override
  public void read(RPCBuffer buffer) {
    mValue = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mValue);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_NUMLINKS;
  }

  public long getValue() {
    return mValue;
  }

  public void setValue(int value) {
    this.mValue = value;
  }
  
}
