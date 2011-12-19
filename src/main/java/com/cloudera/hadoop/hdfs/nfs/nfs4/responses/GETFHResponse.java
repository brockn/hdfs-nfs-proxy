package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class GETFHResponse extends OperationResponse implements Status {
  
  protected int mStatus;
  protected FileHandle mFileHandle;

  @Override
  public void read(RPCBuffer buffer) {
    reset();
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      mFileHandle = new FileHandle();
      mFileHandle.read(buffer);
    }
  }

  protected void reset() {
    mFileHandle = null;    
  }
  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mFileHandle != null) {
      mFileHandle.write(buffer);
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
    return NFS4_OP_GETFH;
  }

  public FileHandle getFileHandle() {
    return mFileHandle;
  }

  public void setFileHandle(FileHandle fileHandle) {
    this.mFileHandle = fileHandle;
  }
  
}
