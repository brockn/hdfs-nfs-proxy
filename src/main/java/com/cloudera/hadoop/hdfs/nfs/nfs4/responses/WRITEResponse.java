package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class WRITEResponse extends OperationResponse implements Status {
  
  protected int mStatus;
  protected int mCount;
  protected int mCommitted;
  protected OpaqueData8 mVerifer;
  
  
    @Override
  public void read(RPCBuffer buffer) {
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      mCount = buffer.readUint32();
      mCommitted = buffer.readUint32();
      mVerifer = new OpaqueData8();
      mVerifer.read(buffer);
    }
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mStatus == NFS4_OK) {
      buffer.writeUint32(mCount);
      buffer.writeUint32(mCommitted);
      mVerifer.write(buffer);
    }    
  }
  

  @Override
  public int getStatus() {
    return mStatus;
  }
  @Override
  public void setStatus(int status) {
    this.mStatus = status;
  }
  @Override
  public int getID() {
    return NFS4_OP_WRITE;
  }

  public int getCount() {
    return mCount;
  }

  public void setCount(int count) {
    this.mCount = count;
  }

  public int getCommitted() {
    return mCommitted;
  }

  public void setCommitted(int committed) {
    this.mCommitted = committed;
  }

  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }
  
  
}
