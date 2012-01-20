/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.io.IOException;
import java.net.InetAddress;

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
  NFS4Handler mNFSServer;
  RPCServer<CompoundRequest, CompoundResponse> mRPCServer;
  
  public static void main(String[] args) throws Exception {
    
    System.setProperty("java.security.krb5.realm", "LOCALDOMAIN");
    System.setProperty("java.security.krb5.kdc", "localhost");
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    System.setProperty("java.security.auth.login.config", "sec.conf");
    
    System.exit(ToolRunner.run(new Configuration(), new NFS4Server(), args));
  }
  
  public void start(InetAddress address, int port) throws IOException {
    mNFSServer = new NFS4Handler(getConf());
    mRPCServer = new RPCServer<CompoundRequest, CompoundResponse>(mNFSServer, getConf(), address, port);
    mRPCServer.start();    
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
    start(null, port);
    while(mRPCServer.isAlive()) {
      Thread.sleep(10000L);
    }
    return 0;
  }
  
  public boolean isAlive() {
    return mRPCServer.isAlive();
  }
  public void shutdown() throws IOException {
    mRPCServer.interrupt();
    mRPCServer.shutdown();
    mNFSServer.shutdown();
  }
  public int getPort() {
    return mRPCServer.getPort();
  }

}
