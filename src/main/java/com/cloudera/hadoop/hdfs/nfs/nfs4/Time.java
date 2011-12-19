package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class Time implements MessageBase {
  protected long mSeconds;
  protected int mNanoSeconds;
  public Time() {
    this(0, 0);
  }
  public Time(long ms) {
    this((ms / 1000L), (int)(ms % 1000L * 1000000L));
  }
  public Time(long seconds, int nanoSeconds) {
    this.mSeconds = seconds;
    this.mNanoSeconds = nanoSeconds;
  }
  @Override
  public void read(RPCBuffer buffer) {
    mSeconds = buffer.readUint64();
    mNanoSeconds = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mSeconds);
    buffer.writeUint32(mNanoSeconds);
  }
  public long getSeconds() {
    return mSeconds;
  }
  public void setSeconds(long mSeconds) {
    this.mSeconds = mSeconds;
  }
  public int getNanoSeconds() {
    return mNanoSeconds;
  }
  public void setNanoSeconds(int mNanoSeconds) {
    this.mNanoSeconds = mNanoSeconds;
  }
  
  public long toMilliseconds() {
    long time = mSeconds * 1000L;
    time += (long)mNanoSeconds / 1000000L;
    return time;
  }
}
