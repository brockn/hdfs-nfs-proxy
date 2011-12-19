package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.PathUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class LOOKUPRequest extends OperationRequest {

  protected String mName;
  @Override
  public void read(RPCBuffer buffer) {
    mName = checkPath(buffer.readString());
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeString(mName);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_LOOKUP;
  }
  
  public String getName() {
    return mName;
  }
  public void setName(String name) {
    mName = name;
  }
}
