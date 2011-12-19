package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Time;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class AccessTime extends Attribute {
  protected Time mTime;
  @Override
  public void read(RPCBuffer buffer) {
    mTime = new Time();
    mTime.read(buffer);            

  }

  @Override
  public void write(RPCBuffer buffer) {
    mTime.write(buffer);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_TIME_ACCESS;
  }

  public Time getTime() {
    return mTime;
  }

  public void setTime(Time time) {
    this.mTime = time;
  }
}
