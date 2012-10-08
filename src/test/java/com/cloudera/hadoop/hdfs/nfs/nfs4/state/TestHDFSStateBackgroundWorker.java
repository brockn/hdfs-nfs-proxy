/**
 * Copyright 2012 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
public class TestHDFSStateBackgroundWorker {

  private Map<HDFSOutputStream, WriteOrderHandler> writerOrderHandlerMap;
  private  ConcurrentMap<FileHandle, HDFSFile> fileHandleMap;
  private HDFSStateBackgroundWorker worker;
  
  private long interval;
  private long maxInactivity;
  private HDFSOutputStream out;
  private WriteOrderHandler writeOrderHandler;
  private FileHandle fileHandle;
  private HDFSFile hdfsFile;
  
  @Before
  public void setUp() throws Exception {
    writerOrderHandlerMap = Maps.newHashMap();
    fileHandleMap = Maps.newConcurrentMap();

    out = mock(HDFSOutputStream.class);
    writeOrderHandler = mock(WriteOrderHandler.class);
    fileHandle = new FileHandle("fh".getBytes(Charsets.UTF_8));
    hdfsFile = mock(HDFSFile.class);
    
    interval = 5L;
    maxInactivity = 10L;
    worker = new HDFSStateBackgroundWorker(writerOrderHandlerMap, fileHandleMap, interval, maxInactivity);
    worker.setDaemon(true);
    worker.start();
    while(!worker.isAlive()) {
      Thread.sleep(1L);
    }
  }

  @After
  public void tearDown() throws Exception {
    worker.shutdown();
  }

  @Test
  public void testShutdown() throws Exception {
    worker.shutdown();
    while(worker.isAlive()) {
      Thread.sleep(1L);
    }
  }
  @Test
  public void testDoesNotRemoveActiveWriters() throws Exception {
    when(out.getLastOperation()).thenAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return System.currentTimeMillis();
      }
    });
    synchronized (writerOrderHandlerMap) {
      writerOrderHandlerMap.put(out, writeOrderHandler);
    }
    Thread.sleep(maxInactivity * 2);
    synchronized (writerOrderHandlerMap) {
      Assert.assertTrue(writerOrderHandlerMap.containsKey(out));
    }
    verify(writeOrderHandler, never()).close(true);
  }
  @Test
  public void testDoesRemoveInactiveWriters() throws Exception {
    when(out.getLastOperation()).thenReturn(System.currentTimeMillis());
    synchronized (writerOrderHandlerMap) {
      writerOrderHandlerMap.put(out, writeOrderHandler);
    }
    Thread.sleep(maxInactivity * 2);
    synchronized (writerOrderHandlerMap) {
      Assert.assertFalse(writerOrderHandlerMap.containsKey(out));
    }
    verify(writeOrderHandler).close(true);
  }
  @Test
  public void testFileNotOpen() throws Exception {
    when(hdfsFile.isOpen()).thenReturn(false);
    synchronized (fileHandleMap) {
      fileHandleMap.put(fileHandle, hdfsFile);
    }
    Thread.sleep(interval * 2);
    verify(hdfsFile, never()).closeResourcesInactiveSince(any(Long.class));
  }
  @Test
  public void testFileOpen() throws Exception {
    when(hdfsFile.isOpen()).thenReturn(true);
    synchronized (fileHandleMap) {
      fileHandleMap.put(fileHandle, hdfsFile);
    }
    Thread.sleep(interval * 2);
    verify(hdfsFile).closeResourcesInactiveSince(any(Long.class));
  }
}
