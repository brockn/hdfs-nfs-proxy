package com.cloudera.hadoop.hdfs.nfs.nfs4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCServer;

public class NFS4Server extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new NFS4Server(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    int port = 2049;
    if(args.length > 0) {
      port = Integer.parseInt(args[0]);
    }
    NFS4Handler server = new NFS4Handler(getConf());
    RPCServer<CompoundRequest, CompoundResponse> rpcServer = new RPCServer<CompoundRequest, CompoundResponse>(server, getConf(), port);
    rpcServer.start();
    while(rpcServer.isAlive()) {
      Thread.sleep(10000L);
    }
    return 0;
  }

}
