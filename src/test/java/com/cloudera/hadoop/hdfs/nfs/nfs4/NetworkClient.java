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


import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCTestUtil;
import com.cloudera.hadoop.hdfs.nfs.security.Credentials;

public class NetworkClient extends BaseClient {

  protected NFS4Server mNFS4Server;
  protected Socket mClient;
  protected InputStream mInputStream;
  protected OutputStream mOutputStream;
  protected int mPort;
  public NetworkClient() throws IOException, NFS4Exception {    
    Configuration conf = new Configuration();
    mNFS4Server = new NFS4Server();
    mNFS4Server.setConf(conf);
    mNFS4Server.start(0);
    mPort = mNFS4Server.getPort();
    
    mClient = new Socket("localhost", mPort);
    
    mInputStream = mClient.getInputStream();
    mOutputStream = mClient.getOutputStream();
    
    initialize();

  }
  
  @Override
  protected CompoundResponse doMakeRequest(CompoundRequest request) throws IOException {
    
    RPCBuffer buffer = new RPCBuffer();
    RPCRequest rpcRequest = RPCTestUtil.createRequest();
    rpcRequest.setCredentials((Credentials)TestUtils.newCredentials());
    rpcRequest.write(buffer);
    request.write(buffer);
    
    buffer.write(mOutputStream);
    mOutputStream.flush();
    
    buffer = RPCBuffer.from(mInputStream);
    RPCResponse rpcResponse = new RPCResponse();
    rpcResponse.read(buffer);
    
    assertEquals(rpcRequest.getXid(), rpcResponse.getXid());
    assertEquals(RPC_REPLY_STATE_ACCEPT, rpcResponse.getReplyState());
    assertEquals(RPC_ACCEPT_SUCCESS, rpcResponse.getAcceptState());
    
    CompoundResponse response = new CompoundResponse();
    response.read(buffer);
    return response;
  }
  
  @Override
  public void shutdown() {
    try {
      mNFS4Server.shutdown();
    } catch(Exception ex) {}
    try {
      mClient.close();
    } catch(Exception ex) {}
  }

}
