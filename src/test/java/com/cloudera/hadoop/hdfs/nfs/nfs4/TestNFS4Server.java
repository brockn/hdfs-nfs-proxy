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
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCTestUtil;

public class TestNFS4Server {

  NFS4Server mNFS4Server;
  int mPort;
  
  @Before
  public void setup() throws Exception {
    Configuration conf = TestUtils.setupConf();
    mNFS4Server = new NFS4Server();
    mNFS4Server.setConf(conf);
    mNFS4Server.start(LOCALHOST, 0);
    mPort = mNFS4Server.getPort();
  }
  
  @After
  public void cleanup() {
    if(mNFS4Server != null) {
      try {
        mNFS4Server.shutdown();
      } catch(Exception ex) {}
    }
  }
  @Test
  public void testNull() throws UnknownHostException, IOException {
    assertTrue(mNFS4Server.isAlive());
    RPCRequest request = RPCTestUtil.createRequest();
    request.setProcedure(NFS_PROC_NULL);
    RPCBuffer buffer = new RPCBuffer();
    request.write(buffer);
    
    Socket socket = new Socket(LOCALHOST, mPort);
    try {
      OutputStream out = socket.getOutputStream();
      InputStream in = socket.getInputStream();
      
      buffer.write(out);
      
      buffer = RPCBuffer.from(in);
      RPCResponse response = new RPCResponse();
      response.read(buffer);
      assertEquals(request.getXid(), response.getXid());
      assertEquals(RPC_REPLY_STATE_ACCEPT, response.getReplyState());
      assertEquals(RPC_ACCEPT_SUCCESS, response.getAcceptState());
    } finally {
      try {
        socket.close();
      } catch(Exception ex) {}
    }
  }
  
  /**
   * Test to ensure most of the time we are able to receive a packet 
   * after disconnect. Why most of the time? There is a race in that
   * the server will not always throw an exception when trying to write
   * the packet after we have stopped listening. 
   */
  @Test
  public void testDiscconnectReconnect() throws UnknownHostException, IOException {
    int attempts = 10, maxFailures = 5, failures = 0;
    for (int i = 0; i < attempts; i++) {
      try {
        doDiscconnectReconnect();
      } catch(Exception ex) {
        failures++;
      }
    }
    assertTrue("failures = " + failures, failures <= maxFailures);
  }
  
  protected void doDiscconnectReconnect() throws UnknownHostException, IOException {
    assertTrue(mNFS4Server.isAlive());
    RPCRequest request = RPCTestUtil.createRequest();
    request.setProcedure(NFS_PROC_NULL);
    RPCBuffer buffer = new RPCBuffer();
    request.write(buffer);
    
    Socket socket = new Socket(LOCALHOST, mPort);
    socket.setTcpNoDelay(true);
    socket.setPerformancePreferences(0, 1, 0);
    socket.setSoTimeout(2000);
    try {
      OutputStream out = socket.getOutputStream();
      InputStream in = socket.getInputStream();
      
      buffer.write(out);
      out.flush();    
      in.close();
      out.close();
      socket.close();
      
      socket = new Socket(LOCALHOST, mPort);
      socket.setTcpNoDelay(true);
      socket.setPerformancePreferences(0, 1, 0);
      socket.setSoTimeout(2000);
      out = socket.getOutputStream();
      in = socket.getInputStream();
      
      buffer = RPCBuffer.from(in);
      RPCResponse response = new RPCResponse();
      response.read(buffer);
      assertEquals(request.getXid(), response.getXid());
      assertEquals(RPC_REPLY_STATE_ACCEPT, response.getReplyState());
      assertEquals(RPC_ACCEPT_SUCCESS, response.getAcceptState());
    } finally {
      try {
        socket.close();
      } catch(Exception ex) {}
    }
  }
}
