package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class RENEWRequest extends OperationRequest {
  protected long mClientID;
  
  @Override
  public void read(RPCBuffer buffer) {
    mClientID = buffer.readLong();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeLong(mClientID);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_RENEW;
  }

  public long getClientID() {
    return mClientID;
  }

  public void setClientID(long clientID) {
    this.mClientID = clientID;
  }
}
