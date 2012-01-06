package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;

public class LocalClient extends BaseClient {

  protected NFS4Handler mServer = new NFS4Handler();
  
  public LocalClient() {
    initialize();
  }
  
  @Override
  protected CompoundResponse doMakeRequest(CompoundRequest request) {
    CompoundResponse response = mServer.process(new RPCRequest(), request, "localhost.localdomain", "test");
    return response;
  }

}
