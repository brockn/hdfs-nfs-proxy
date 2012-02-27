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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.GETATTRRequest;

public class TestGETATTRHandler {

  GETATTRHandler handler;
  NFS4Handler server;
  Session session;
  GETATTRRequest request;
  FileSystem fs;
  FileHandle fileHandle = new FileHandle("fileHandle".getBytes());

  @Before
  public void setup() throws NFS4Exception {
    handler = new GETATTRHandler();
    server = mock(NFS4Handler.class);
    session = mock(Session.class);
    request = new GETATTRRequest();
    fs = mock(FileSystem.class);
    when(session.getFileSystem()).thenReturn(fs);
  }

  @Test
  public void testFileNotFound() throws NFS4Exception, IOException {
    when(server.getPath(any(FileHandle.class))).thenReturn(new Path("/"));
    when(session.getCurrentFileHandle()).thenReturn(fileHandle);
    when(fs.getFileStatus(any(Path.class))).thenThrow(new FileNotFoundException());
    Status response = handler.handle(server, session, request);
    assertEquals(NFS4ERR_NOENT, response.getStatus());
  }

}
