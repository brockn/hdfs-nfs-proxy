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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileBackedWrite;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MemoryBackedWrite;
import com.cloudera.hadoop.hdfs.nfs.nfs4.PendingWrite;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.WRITERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.WRITEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSOutputStream;
import com.google.common.io.Files;

public class TestWRITEHandler extends TestBaseHandler {

  private WRITEHandler handler;
  private WRITERequest request;
  private Path file;
  File fileBackedWrite;

  private WriteOrderHandler writeOrderHandler;
  private HDFSOutputStream outputStream;
  private AtomicReference<PendingWrite> writeReference;
  
  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    fileBackedWrite = new File(Files.createTempDir(), "file");
    handler = new WRITEHandler();
    request = new WRITERequest();
    writeReference = new AtomicReference<PendingWrite>();
    request.setOffset(0);
    request.setData(new byte[512], 0, 512);
    file = new Path("dir", "file");
    writeOrderHandler = mock(WriteOrderHandler.class);
    outputStream = mock(HDFSOutputStream.class);
    when(hdfsState.getPath(currentFileHandle)).thenReturn(file);
    when(hdfsState.openForWrite(any(StateID.class), any(FileHandle.class), 
        any(Boolean.class))).thenReturn(outputStream);
    when(hdfsState.forWrite(any(StateID.class), any(FileHandle.class))).thenReturn(outputStream);
    when(hdfsState.getOrCreateWriteOrderHandler(currentFileHandle)).thenReturn(writeOrderHandler);
    when(writeOrderHandler.getTemporaryFile(any(String.class))).thenReturn(fileBackedWrite);
    when(writeOrderHandler.write(any(PendingWrite.class))).then(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        writeReference.set((PendingWrite)invocation.getArguments()[0]);
        return writeReference.get().getLength();
      }      
    });
  }
  @After
  public void tearDown() throws IOException {
    PathUtils.fullyDelete(fileBackedWrite.getParentFile());
  }
  @Test
  public void testWouldBlock() throws Exception {
    assertFalse(writeOrderHandler.writeWouldBlock(Long.MAX_VALUE));
  }
  @Test
  public void testBasicWrite() throws Exception {
    WRITEResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    assertEquals(request.getLength(), response.getCount());
    PendingWrite write = writeReference.get();
    assertNotNull(write);
    assertTrue(write instanceof MemoryBackedWrite);
  }
  
  @Test
  public void testSyncRandomWrite() throws Exception {
    when(writeOrderHandler.writeWouldBlock(any(Long.class))).thenReturn(true);
    request.setOffset(1);
    request.setStable(NFS4_COMMIT_DATA_SYNC4);
    WRITEResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    assertEquals(request.getLength(), response.getCount());
    PendingWrite write = writeReference.get();
    assertNotNull(write);
    assertFalse(write.isSync());
  }
  
  @Test
  public void testSyncNotBlock() throws Exception {
    when(writeOrderHandler.writeWouldBlock(any(Long.class))).thenReturn(false);
    request.setStable(NFS4_COMMIT_DATA_SYNC4);
    WRITEResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    assertEquals(request.getLength(), response.getCount());
    PendingWrite write = writeReference.get();
    assertNotNull(write);
    assertTrue(write.isSync());
  }
  
  @Test
  public void testFileBackedWrite() throws Exception {
    request.setOffset(ONE_MB * 2);
    WRITEResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    assertEquals(request.getLength(), response.getCount());
    PendingWrite write = writeReference.get();
    assertNotNull(write);
    assertTrue(write instanceof FileBackedWrite);
  }

}
