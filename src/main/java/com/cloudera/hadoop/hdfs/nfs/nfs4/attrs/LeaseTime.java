package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class LeaseTime extends Attribute {
  public LeaseTime() {
    super();
  }
  protected int mSeconds;
  @Override
  public void read(RPCBuffer buffer) {
    mSeconds = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mSeconds);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_LEASE_TIME;
  }

  public int getSeconds() {
    return mSeconds;
  }

  public void setSeconds(int seconds) {
    this.mSeconds = seconds;
  }

  
}
