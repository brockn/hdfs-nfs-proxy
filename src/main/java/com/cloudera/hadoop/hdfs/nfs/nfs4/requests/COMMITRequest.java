package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class COMMITRequest extends OperationRequest {
  protected long mOffset;
  protected int mCount;
  
  @Override
  public void read(RPCBuffer buffer) {
    mOffset = buffer.readUint64();
    mCount = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mOffset);
    buffer.writeUint32(mCount);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_COMMIT;
  }

  public long getOffset() {
    return mOffset;
  }

  public void setOffset(long offset) {
    this.mOffset = offset;
  }

  public int getCount() {
    return mCount;
  }

  public void setCount(int count) {
    this.mCount = count;
  }


}
