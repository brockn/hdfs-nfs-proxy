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
package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.TestUtils.copy;
import static com.cloudera.hadoop.hdfs.nfs.TestUtils.deepEquals;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_REJECT_AUTH_ERROR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_REPLY_STATE_DENIED;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.security.Credentials;

public class TestRPC {

  @Test
  public void testRequestWire() throws Exception {
    RPCRequest base = RPCTestUtil.createRequest();
    RPCRequest copy = new RPCRequest();
    copy(base, copy);
    assertEquals(base.getMessageType(), copy.getMessageType());
    assertEquals(base.getRpcVersion(), copy.getRpcVersion());
    assertEquals(base.getProgram(), copy.getProgram());
    assertEquals(base.getProgramVersion(), copy.getProgramVersion());
    assertEquals(base.getProcedure(), copy.getProcedure());
    assertEquals(base.getCredentials().getFlavor(),
        copy.getCredentials().getFlavor());
    deepEquals(base, copy);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnknownCreds() throws Exception {
    RPCRequest request = RPCTestUtil.createRequest();
    Credentials creds = mock(Credentials.class);
    when(creds.getFlavor()).thenReturn(Integer.MAX_VALUE);
    request.setCredentials(creds);
    copy(request, new RPCRequest());
  }

  @Test
  public void testResponseWire() throws Exception {
    RPCResponse base = RPCTestUtil.createResponse();
    RPCResponse copy = new RPCResponse();
    copy(base, copy);
    assertEquals(base.getMessageType(), copy.getMessageType());
    assertEquals(base.getReplyState(), copy.getReplyState());
    assertEquals(base.getAcceptState(), copy.getAcceptState());
    deepEquals(base, copy);
  }

  @Test
  public void testResponseWireAuthError() throws Exception {
    RPCResponse base = RPCTestUtil.createResponse();
    base.setReplyState(RPC_REPLY_STATE_DENIED);
    base.setAuthState(RPC_REJECT_AUTH_ERROR);
    RPCResponse copy = new RPCResponse();
    copy(base, copy);
    assertEquals(base.getMessageType(), copy.getMessageType());
    assertEquals(base.getReplyState(), copy.getReplyState());
    assertEquals(base.getAcceptState(), copy.getAcceptState());
    deepEquals(base, copy);
  }

}
