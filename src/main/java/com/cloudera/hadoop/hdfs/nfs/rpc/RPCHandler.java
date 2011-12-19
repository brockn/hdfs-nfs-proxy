package com.cloudera.hadoop.hdfs.nfs.rpc;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;


public abstract class RPCHandler<REQUEST extends MessageBase, RESPONSE extends MessageBase> {

  public abstract RESPONSE process(final RPCRequest rpcRequest, final REQUEST request, final String clientHostPort, final String sessionID);
  
  public abstract RESPONSE createResponse();
  
  public abstract REQUEST createRequest();
  
  public abstract void incrementMetric(String name, long count);
  
  
}
