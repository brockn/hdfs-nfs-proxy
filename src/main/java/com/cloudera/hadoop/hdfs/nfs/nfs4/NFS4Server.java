package com.cloudera.hadoop.hdfs.nfs.nfs4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCServer;

/**
 * Class used to start the NFS Server. Uses NFS4Handler
 * and RPCServer to start the server and then blocks
 * until the RPCServer has died. Implements Configured
 * so it can be configured from the command line.
 */
public class NFS4Server extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new NFS4Server(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    int port;
    try {
      port = Integer.parseInt(args[0]);
    } catch(Exception e) {
      System.err.println(this.getClass().getName() + " port");
      GenericOptionsParser.printGenericCommandUsage(System.err);
      return 1;
    }
    NFS4Handler server = new NFS4Handler(getConf());
    RPCServer<CompoundRequest, CompoundResponse> rpcServer = 
        new RPCServer<CompoundRequest, CompoundResponse>(server, getConf(), port);
    rpcServer.start();
    while(rpcServer.isAlive()) {
      Thread.sleep(10000L);
    }
    return 0;
  }

}
