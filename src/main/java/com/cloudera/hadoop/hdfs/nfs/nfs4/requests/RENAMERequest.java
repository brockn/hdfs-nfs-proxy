package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.PathUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class RENAMERequest extends OperationRequest {

  protected String mOldName;
  protected String mNewName;
  @Override
  public void read(RPCBuffer buffer) {
    mOldName = checkPath(buffer.readString());
    mNewName = checkPath(buffer.readString());
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeString(mOldName);
    buffer.writeString(mNewName);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_RENAME;
  }
  
  public String getOldName() {
    return mOldName;
  }
  public void setOldName(String name) {
    mOldName = name;
  }
  
  public String getNewName() {
    return mNewName;
  }
  public void setNewName(String name) {
    mNewName = name;
  }
  
}
