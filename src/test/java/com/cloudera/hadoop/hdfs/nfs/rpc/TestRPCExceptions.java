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
package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import junit.framework.Assert;

import org.junit.Test;

public class TestRPCExceptions {

  @Test
  public void testAccepted() throws Exception {
    Throwable t = new Throwable();
    RPCAcceptedException ex = new RPCAcceptedException(99, t);
    Assert.assertEquals(RPC_REPLY_STATE_ACCEPT, ex.getReplyState());
    Assert.assertEquals(99, ex.getAcceptState());
    Assert.assertNotNull(ex.getMessage());
    Assert.assertSame(t, ex.getCause());
  }


  @Test
  public void testDenied() throws Exception {
    Throwable t = new Throwable();
    RPCDeniedException ex = new RPCDeniedException(99, t);
    Assert.assertEquals(RPC_REPLY_STATE_DENIED, ex.getReplyState());
    Assert.assertEquals(99, ex.getAcceptState());
    Assert.assertNotNull(ex.getMessage());
    Assert.assertSame(t, ex.getCause());
  }

  @Test
  public void testAuth() throws Exception {
    Throwable t = new Throwable();
    RPCAuthException ex = new RPCAuthException(99, t);
    Assert.assertEquals(RPC_REPLY_STATE_DENIED, ex.getReplyState());
    Assert.assertEquals(RPC_REJECT_AUTH_ERROR, ex.getAcceptState());
    Assert.assertEquals(99, ex.getAuthState());
    Assert.assertNotNull(ex.getMessage());
    Assert.assertSame(t, ex.getCause());
  }
  @Test
  public void testAuthNoCause() throws Exception {
    RPCAuthException ex = new RPCAuthException(99);
    Assert.assertEquals(RPC_REPLY_STATE_DENIED, ex.getReplyState());
    Assert.assertEquals(RPC_REJECT_AUTH_ERROR, ex.getAcceptState());
    Assert.assertEquals(99, ex.getAuthState());
    Assert.assertNotNull(ex.getMessage());
    Assert.assertNull(ex.getCause());
  }
  @Test
  public void testAcceptedNoCause() throws Exception {
    RPCAcceptedException ex = new RPCAcceptedException(99);
    Assert.assertEquals(RPC_REPLY_STATE_ACCEPT, ex.getReplyState());
    Assert.assertEquals(99, ex.getAcceptState());
    Assert.assertNotNull(ex.getMessage());
    Assert.assertNull(ex.getCause());
  }
  @Test
  public void testDeniedNoCause() throws Exception {
    RPCDeniedException ex = new RPCDeniedException(99);
    Assert.assertEquals(RPC_REPLY_STATE_DENIED, ex.getReplyState());
    Assert.assertEquals(99, ex.getAcceptState());
    Assert.assertNotNull(ex.getMessage());
    Assert.assertNull(ex.getCause());
  }
}
