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
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OPENCONFIRMRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OPENCONFIRMResponse;

public class TestOPENCONFIRMHandler extends TestBaseHandler {

  private OPENCONFIRMHandler handler;
  private OPENCONFIRMRequest request;

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new OPENCONFIRMHandler();
    request = new OPENCONFIRMRequest();
  }

  @Test
  public void testSuccess() throws Exception {
    StateID stateID = new StateID();
    when(hdfsState.confirm(any(StateID.class), any(Integer.class), any(FileHandle.class))).thenReturn(stateID);
    OPENCONFIRMResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    Assert.assertEquals(stateID, response.getStateID());
  }
}
