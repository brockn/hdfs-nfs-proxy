package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Time;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class SetModifyTime extends Attribute {
  protected int mHow;
  protected Time mTime;
  @Override
  public void read(RPCBuffer buffer) {
    mHow = buffer.readUint32();
    if(mHow == NFS4_SET_TO_CLIENT_TIME4) {
      mTime = new Time();
      mTime.read(buffer);            
    }
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mHow);
    if(mHow == NFS4_SET_TO_CLIENT_TIME4) {
      mTime.write(buffer);
    }
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_TIME_MODIFY_SET;
  }

  public Time getTime() {
    return mTime;
  }

  public void setTime(Time time) {
    this.mTime = time;
  }  
  
  public int getHow() {
    return mHow;
  }
  public void setHow(int how) {
    mHow = how;
  }
}
