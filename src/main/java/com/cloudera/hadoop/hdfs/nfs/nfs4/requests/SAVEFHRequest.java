package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class SAVEFHRequest extends OperationRequest {

  @Override
  public void read(RPCBuffer buffer) {
    
  }

  @Override
  public void write(RPCBuffer buffer) {
    
  }
  
  @Override
  public int getID() {
    return NFS4_OP_SAVEFH;
  }
}
