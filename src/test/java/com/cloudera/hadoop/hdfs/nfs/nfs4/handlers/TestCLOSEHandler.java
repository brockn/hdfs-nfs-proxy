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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CLOSERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CLOSEResponse;

public class TestCLOSEHandler extends TestBaseHandler {

  private CLOSEHandler handler;
  private CLOSERequest request;
  
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new CLOSEHandler();
    request = new CLOSERequest();
  }
  @Test
  public void testSuccess() throws Exception {
    StateID stateID = new StateID();
    when(hdfsState.close(any(String.class), any(StateID.class), any(Integer.class), 
        any(FileHandle.class))).thenReturn(stateID);
    CLOSEResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    Assert.assertSame(stateID, response.getStateID());
  }
  @Test
  public void testWouldBlockIOException() throws Exception {
    when(hdfsState.closeWouldBlock(currentFileHandle)).thenThrow(new IOException());
    Assert.assertFalse(handler.wouldBlock(hdfsState, session, request));
  }
  @Test
  public void testWouldBlock() throws Exception {
    when(hdfsState.closeWouldBlock(currentFileHandle)).thenReturn(true);
    Assert.assertTrue(handler.wouldBlock(hdfsState, session, request));
  }
  @Test
  public void testWouldNotBlock() throws Exception {
    when(hdfsState.closeWouldBlock(currentFileHandle)).thenReturn(false);
    Assert.assertFalse(handler.wouldBlock(hdfsState, session, request));
  }
}
