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

import java.io.FileNotFoundException;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RENAMERequest;

public class TestRENAMEHandler extends TestBaseHandler {

  private RENAMEHandler handler;
  private RENAMERequest request;
  private final Path oldPath = new Path("/old", "a");
  private final Path newPath = new Path("/new", "b");
  
  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new RENAMEHandler();
    request = new RENAMERequest();
    request.setOldName(oldPath.getName());
    request.setNewName(newPath.getName());

    when(session.getSavedFileHandle()).thenReturn(savedFileHandle);
    
    when(hdfsState.getPath(currentFileHandle)).thenReturn(newPath.getParent());
    when(hdfsState.getPath(savedFileHandle)).thenReturn(oldPath.getParent());
    
    when(fs.getFileStatus(newPath.getParent())).thenReturn(isdir);
    when(fs.getFileStatus(oldPath.getParent())).thenReturn(isdir);
    
    when(hdfsState.fileExists(oldPath)).thenReturn(true);
    when(hdfsState.fileExists(newPath)).thenReturn(false);

    when(fs.rename(oldPath, newPath)).thenReturn(true);
  }
  @Test
  public void testFileNotFound() throws Exception {
    when(fs.getFileStatus(any(Path.class))).thenThrow(new FileNotFoundException());
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOENT, response.getStatus());
  }
  @Test
  public void testInterruptWithOpenFile() throws Exception {
    when(hdfsState.isFileOpen(oldPath)).thenReturn(true);
    Thread.currentThread().interrupt();
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_IO, response.getStatus());
  }
  @Test
  public void testInvalidNewName() throws Exception {
    request.setNewName("");
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
    
    request.setNewName(null);
    response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
  }
  @Test
  public void testInvalidOldName() throws Exception {
    request.setOldName("");
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
    
    request.setOldName(null);
    response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
  }
  @Test
  public void testNewDirIsNotDir() throws Exception {
    when(fs.getFileStatus(newPath.getParent())).thenReturn(notdir);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOTDIR, response.getStatus());
  }
  @Test
  public void testNewPathExists() throws Exception {
    when(hdfsState.fileExists(newPath)).thenReturn(true);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_EXIST, response.getStatus());
  }
  @Test
  public void testNoCurrentFileHandle() throws Exception {
    when(session.getCurrentFileHandle()).thenReturn(null);
    when(session.getSavedFileHandle()).thenReturn(savedFileHandle);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOFILEHANDLE, response.getStatus());
  }
  @Test
  public void testNoSavedFileHandle() throws Exception {
    when(session.getCurrentFileHandle()).thenReturn(currentFileHandle);
    when(session.getSavedFileHandle()).thenReturn(null);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOFILEHANDLE, response.getStatus());
  }
  @Test
  public void testOldDirIsNotDir() throws Exception {
    when(fs.getFileStatus(oldPath.getParent())).thenReturn(notdir);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOTDIR, response.getStatus());
  }
  @Test
  public void testOldPathDoesNotExist() throws Exception {
    when(hdfsState.fileExists(oldPath)).thenReturn(false);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOENT, response.getStatus());
  }
  @Test
  public void testOldPathIsOpen() throws Exception {
    when(hdfsState.isFileOpen(oldPath)).thenReturn(true);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_FILE_OPEN, response.getStatus());
  }
  @Test
  public void testRenameReturnsFalse() throws Exception {
    when(fs.rename(oldPath, newPath)).thenReturn(false);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_IO, response.getStatus());
  }
  @Test
  public void testSuccess() throws Exception {
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
  }
}
