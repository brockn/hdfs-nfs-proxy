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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.READRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.READResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSInputStream;

public class TestREADHandler extends TestBaseHandler {

  private READHandler handler;
  private READRequest request;
  private Path file;

  private HDFSInputStream inputStream;

  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new READHandler();
    request = new READRequest();
    request.setOffset(512);
    request.setCount(512);
    file = new Path("dir", "file");
    inputStream = mock(HDFSInputStream.class);
    when(inputStream.getPos()).thenReturn(512L);
    when(inputStream.read(any(byte[].class))).thenReturn(512);
    when(hdfsState.getPath(currentFileHandle)).thenReturn(file);
    when(hdfsState.forRead(any(StateID.class), any(FileSystem.class), any(FileHandle.class))).
      thenReturn(inputStream);
  }
  @Test
  public void testInvalidOffset() throws Exception {
    request.setCount(-1);
    READResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
  }
  @Test
  public void testSeekIOException() throws Exception {
    request.setOffset(0);
    doThrow(new IOException("Injected")).when(inputStream).seek(any(Long.class));
    READResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_IO, response.getStatus());
  }
  @Test
  public void testSeek() throws Exception {
    request.setOffset(1);
    READResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    verify(inputStream).seek(1);
  }
  @Test
  public void testEOF() throws Exception {
    when(inputStream.read(any(byte[].class))).thenReturn(-1);
    READResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    assertTrue(response.isEOF());
    verify(inputStream).read(any(byte[].class));
  }
  @Test
  public void testSuccess() throws Exception {
    READResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    assertFalse(response.isEOF());
    assertEquals(512, response.getLength());
    verify(inputStream).read(any(byte[].class));
  }
}
