package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class PUTFHRequest extends OperationRequest {
  protected FileHandle mFileHandle;
  @Override
  public void read(RPCBuffer buffer) {
    mFileHandle = new FileHandle();
    mFileHandle.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    mFileHandle.write(buffer);
  }

  @Override
  public int getID() {
    return NFS4_OP_PUTFH;
  }

  public FileHandle getFileHandle() {
    return mFileHandle;
  }

  public void setFileHandle(FileHandle fileHandle) {
    this.mFileHandle = fileHandle;
  }
  
  
}
