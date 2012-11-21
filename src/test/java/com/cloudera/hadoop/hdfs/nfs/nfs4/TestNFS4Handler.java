/**
 * Copyright 2012 Cloudera Inc.
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
import static org.mockito.Mockito.*;

import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCAcceptedException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCTestUtil;
import com.cloudera.hadoop.hdfs.nfs.security.AccessPrivilege;
import com.cloudera.hadoop.hdfs.nfs.security.SecurityHandlerFactory;

public class TestNFS4Handler {

  private NFS4Handler nfs4Handler;
  private SecurityHandlerFactory securityHandlerFactory;
  private RPCRequest rpcRequest;
  private CompoundRequest compoundRequest;
  private InetAddress clientAddress;
  private AccessPrivilege accessPrivilege;
  private String sessionID;
 
  @Before
  public void setup() throws Exception {
    Configuration conf = TestUtils.setupConf();
    securityHandlerFactory = mock(SecurityHandlerFactory.class);
    nfs4Handler = new NFS4Handler(conf, securityHandlerFactory);
    rpcRequest = RPCTestUtil.createRequest();
    compoundRequest = new CompoundRequest();
    clientAddress = InetAddress.getLocalHost();
    accessPrivilege = AccessPrivilege.READ_WRITE;
    sessionID = "dummmy";
  }
  @After
  public void cleanup() {
    if(nfs4Handler != null) {
      try {
        nfs4Handler.shutdown();
      } catch(Exception ex) {}
    }
  }
  @Test
  public void testBadProgram() throws Exception {
    rpcRequest.setProgram(NFS_PROG + 1);
    try {
      nfs4Handler.beforeProcess(rpcRequest);
      Assert.fail();
    } catch (RPCAcceptedException e) {
      Assert.assertEquals(RPC_ACCEPT_PROG_UNAVAIL, e.getAcceptState());
    }
  }
  @Test
  public void testBadVersion() throws Exception {
    rpcRequest.setProgramVersion(NFS_VERSION + 1);
    try {
      nfs4Handler.beforeProcess(rpcRequest);
      Assert.fail();
    } catch (RPCAcceptedException e) {
      Assert.assertEquals(RPC_ACCEPT_PROG_MISMATCH, e.getAcceptState());
    }
  }
  @Test
  public void testBadProc() throws Exception {
    rpcRequest.setProcedure(NFS_PROC_COMPOUND + 1);
    try {
      nfs4Handler.beforeProcess(rpcRequest);
      Assert.fail();
    } catch (RPCAcceptedException e) {
      Assert.assertEquals(RPC_ACCEPT_PROC_UNAVAIL, e.getAcceptState());
    }
  }
  @Test
  public void testBadMinorVersion() throws Exception {
    compoundRequest.setMinorVersion(NFS_MINOR_VERSION + 1);
    CompoundResponse compoundResponse = nfs4Handler.process(rpcRequest, compoundRequest, 
        accessPrivilege, clientAddress, sessionID).get();
    Assert.assertEquals(NFS4ERR_MINOR_VERS_MISMATCH, compoundResponse.getStatus());
  }
  @Test
  public void testNullCreds() throws Exception {
    compoundRequest.setCredentials(null);
    CompoundResponse compoundResponse = nfs4Handler.process(rpcRequest, compoundRequest, 
        accessPrivilege, clientAddress, sessionID).get();
    Assert.assertEquals(NFS4ERR_WRONGSEC, compoundResponse.getStatus());
  }
}
