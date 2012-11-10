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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OPENRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OPENResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSOutputStream;
import com.google.common.base.Charsets;

public class TestOPENHandler extends TestBaseHandler {

  private OPENHandler handler;
  private OPENRequest request;
  private Path parent;
  private Path file;
  private HDFSOutputStream out;

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new OPENHandler();
    request = new OPENRequest();
    parent = new Path("dir");
    file = new Path(parent, "file");
    request.setName(file.getName());
    out = mock(HDFSOutputStream.class);
    when(hdfsState.openForWrite(any(FileSystem.class), any(StateID.class), 
        any(FileHandle.class), any(Boolean.class))).thenReturn(out);
    when(hdfsState.getPath(currentFileHandle)).thenReturn(parent);
    when(hdfsState.getOrCreateFileHandle(file)).thenReturn(new FileHandle("file".getBytes(Charsets.UTF_8)));
  }
  @Test
  public void testBothReadWrite() throws Exception {
    request.setAccess(NFS4_OPEN4_SHARE_ACCESS_BOTH);
    OPENResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOTSUPP, response.getStatus());
  }
  @Test
  public void testInvalidNameEmpty() throws Exception {
    request.setName("");
    OPENResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
  }
  @Test
  public void testInvalidNameNull() throws Exception {
    request.setName(null);
    OPENResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
  }
  @Test
  public void testRead() throws Exception {
    request.setAccess(NFS4_OPEN4_SHARE_ACCESS_READ);
    OPENResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
  }
  @Test
  public void testReadWithDeny() throws Exception {
    request.setAccess(NFS4_OPEN4_SHARE_ACCESS_READ);
    request.setDeny(1);
    OPENResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOTSUPP, response.getStatus());
  }
  @Test
  public void testWrite() throws Exception {
    request.setAccess(NFS4_OPEN4_SHARE_ACCESS_READ);
    OPENResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
  }
}
