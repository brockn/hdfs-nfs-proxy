/**
 * Copyright 2012 Cloudera Inc.
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

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
public class TestHDFSStateBackgroundWorker {
  protected static final Logger LOGGER = Logger.getLogger(TestHDFSStateBackgroundWorker.class);

  private Map<FileHandle, WriteOrderHandler> writerOrderHandlerMap;
  private ConcurrentMap<FileHandle, HDFSFile> openFileMap;
  private FileHandleINodeMap fileHandleINodeMap;
  private HDFSStateBackgroundWorker worker;
  private File storageDir;
  private long interval;
  private long maxInactivity;
  private long fileHandleReapInterval;
  private HDFSOutputStream out;
  private WriteOrderHandler writeOrderHandler;
  private FileHandle fileHandle;
  private HDFSFile hdfsFile;
  private FileSystem fileSystem;
  private HDFSState hdfsState;
  
  @Before
  public void setUp() throws Exception {
    writerOrderHandlerMap = Maps.newHashMap();
    openFileMap = Maps.newConcurrentMap();
    storageDir = Files.createTempDir();
    fileSystem = mock(FileSystem.class);
    hdfsState = mock(HDFSState.class);
    out = mock(HDFSOutputStream.class);
    writeOrderHandler = mock(WriteOrderHandler.class);
    fileHandle = new FileHandle("fh".getBytes(Charsets.UTF_8));
    hdfsFile = mock(HDFSFile.class);
    openFileMap.put(fileHandle, hdfsFile);
    when(hdfsFile.getHDFSOutputStream()).
      thenReturn(new OpenResource<HDFSOutputStream>(new StateID(), out));
    interval = 5L;
    maxInactivity = 10L;
    fileHandleReapInterval = 2000L;
    fileHandleINodeMap = 
        new FileHandleINodeMap(new File(storageDir, "map"));
    worker = new HDFSStateBackgroundWorker(fileSystem, hdfsState, writerOrderHandlerMap, 
        openFileMap, fileHandleINodeMap, interval, maxInactivity, 
        fileHandleReapInterval);
    worker.setDaemon(true);
    worker.start();
    while(!worker.isAlive()) {
      Thread.sleep(1L);
    }
  }

  @After
  public void tearDown() throws Exception {
    worker.shutdown();
    Thread.sleep(interval * 4L);
    fileHandleINodeMap.close();
    if(storageDir != null) {
      PathUtils.fullyDelete(storageDir);
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
      writerOrderHandlerMap.put(fileHandle, writeOrderHandler);
    }
    Thread.sleep(maxInactivity * 2);
    synchronized (writerOrderHandlerMap) {
      Assert.assertTrue(writerOrderHandlerMap.containsKey(fileHandle));
    }
    verify(writeOrderHandler, never()).close(true);
  }
  @Test
  public void testdoesremoveinactivewriters() throws Exception {
    when(out.getLastOperation()).thenReturn(System.currentTimeMillis());
    synchronized (writerOrderHandlerMap) {
      writerOrderHandlerMap.put(fileHandle, writeOrderHandler);
    }
    Thread.sleep(maxInactivity * 4);
    synchronized (writerOrderHandlerMap) {
      Assert.assertFalse(writerOrderHandlerMap.containsKey(fileHandle));
    }
    verify(writeOrderHandler).close(true);
  }
  @Test
  public void testFileHandleReap() throws Exception {
    FileHandle fh1 = new FileHandle("fh1".getBytes(Charsets.UTF_8));
    FileHandle fh2 = new FileHandle("fh2".getBytes(Charsets.UTF_8));
    FileHandle fh3 = new FileHandle("fh3".getBytes(Charsets.UTF_8));
    // will be deleted
    INode inode1 = new INode("/1", 1);
    // will not be deleted
    INode inode2 = new INode("/1", 2);
    fileHandleINodeMap.put(fh1, inode1);
    fileHandleINodeMap.put(fh2, inode2);
    when(hdfsState.deleteFileHandleFileIfNotExist(fileSystem, fh1)).thenReturn(true);
    when(hdfsState.deleteFileHandleFileIfNotExist(fileSystem, fh2)).thenReturn(false);
    
    Thread.sleep(fileHandleReapInterval + (interval * 2L));
    // will not be returned by getFileHandlesNotValidatedSince
    INode inode3 = new INode("/3", 3);
    fileHandleINodeMap.put(fh3, inode3);
    
    Thread.sleep(interval * 4L);

    verify(hdfsState, atLeast(1)).deleteFileHandleFileIfNotExist(fileSystem, fh1);
    verify(hdfsState, atLeast(1)).deleteFileHandleFileIfNotExist(fileSystem, fh2);
    verify(hdfsState, times(0)).deleteFileHandleFileIfNotExist(fileSystem, fh3);
    INode inode2Updated = fileHandleINodeMap.getINodeByFileHandle(fh2);
    Assert.assertEquals(inode2, inode2Updated);
    Assert.assertTrue(inode2Updated.getCreationTime() > inode2.getCreationTime());
    INode inode3Updated = fileHandleINodeMap.getINodeByFileHandle(fh3);
    Assert.assertEquals(inode3, inode3Updated);
    Assert.assertEquals(inode3.getCreationTime(), inode3Updated.getCreationTime());
  }
  @Test
  public void testFileNotOpen() throws Exception {
    when(hdfsFile.isOpen()).thenReturn(false);
    synchronized (openFileMap) {
      openFileMap.put(fileHandle, hdfsFile);
    }
    Thread.sleep(interval * 2);
    verify(hdfsFile, never()).closeResourcesInactiveSince(any(Long.class));
  }
  @Test
  public void testFileOpen() throws Exception {
    when(hdfsFile.isOpen()).thenReturn(true);
    synchronized (openFileMap) {
      openFileMap.put(fileHandle, hdfsFile);
    }
    Thread.sleep(interval * 2);
    verify(hdfsFile, atLeastOnce()).closeResourcesInactiveSince(any(Long.class));
  }

  @Test
  public void testShutdown() throws Exception {
    worker.shutdown();
    while(worker.isAlive()) {
      Thread.sleep(1L);
    }
  }
}