/**
 * Copyright 2012 The Apache Software Foundation
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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.COMMITRequest;

public class TestCOMMITHandler extends TestBaseHandler {

  private COMMITHandler handler;
  private COMMITRequest request;
  private WriteOrderHandler writeOrderHandler;
  
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new COMMITHandler();
    request = new COMMITRequest();
    
    writeOrderHandler = mock(WriteOrderHandler.class);
    when(hdfsState.forCommit(fs, currentFileHandle)).thenReturn(writeOrderHandler);
  }
  @Test
  public void testZeroOffset() throws Exception {
    request.setOffset(52L);
    Status response = handler.handle(hdfsState, session, request);
    Assert.assertEquals(NFS4_OK, response.getStatus());
    verify(writeOrderHandler, times(1)).sync(52L);
  }
  @Test
  public void testNonZeroOffset() throws Exception {
    when(writeOrderHandler.getCurrentPos()).thenReturn(52L);
    Status response = handler.handle(hdfsState, session, request);
    Assert.assertEquals(NFS4_OK, response.getStatus());
    verify(writeOrderHandler, times(1)).sync(52L);
  }
  @Test
  public void testWouldBlockNFS4Exception() throws Exception {
    when(hdfsState.forCommit(fs, currentFileHandle)).thenThrow(new NFS4Exception(1));
    Assert.assertFalse(handler.wouldBlock(hdfsState, session, request));
  }
  @Test
  public void testWouldBlock() throws Exception {
    when(writeOrderHandler.syncWouldBlock(any(Long.class))).thenReturn(true);
    Assert.assertTrue(handler.wouldBlock(hdfsState, session, request));
  }
  @Test
  public void testWouldNotBlock() throws Exception {
    when(writeOrderHandler.syncWouldBlock(any(Long.class))).thenReturn(false);
    Assert.assertFalse(handler.wouldBlock(hdfsState, session, request));
  }
}
