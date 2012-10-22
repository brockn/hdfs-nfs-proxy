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
import static org.mockito.Mockito.*;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.REMOVERequest;

public class TestREMOVEHandler extends TestBaseHandler {

  private REMOVEHandler handler;
  private REMOVERequest request;  
  private final Path path = new Path("/", "a");
  
  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new REMOVEHandler();
    request = new REMOVERequest();
    
    request.setName(path.getName());
    when(hdfsState.fileExists(path)).thenReturn(true);
    
    when(hdfsState.delete(path)).thenReturn(true);

  }
  @Test
  public void testInvalidName() throws Exception {
    request.setName("");
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
    
    request.setName(null);
    response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
  }
  @Test
  public void testNoCurrentFileHandle() throws Exception {
    when(session.getCurrentFileHandle()).thenReturn(null);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOFILEHANDLE, response.getStatus());
  }
  
  @Test
  public void testPathDoesNotExist() throws Exception {
    when(hdfsState.fileExists(path)).thenReturn(false);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOENT, response.getStatus());
  }
  
  @Test
  public void testDeleteFails() throws Exception {
    when(hdfsState.delete(path)).thenReturn(false);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_PERM, response.getStatus());
  }
  
  @Test
  public void testSuccess() throws Exception {
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
  }
}
