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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.cloudera.hadoop.hdfs.nfs.metrics.LogMetricPublisher;
import com.cloudera.hadoop.hdfs.nfs.metrics.MetricsAccumulator;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData12;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class TestHDFSState {
  protected static final Logger LOGGER = Logger.getLogger(TestHDFSState.class);

  private File storageDir;
  private MetricsAccumulator metrics;
  private FileSystem fs;
  private FileStatus fileStatus;
  private HDFSState hdfsState;
  private StateID stateID1, stateID2;
  private Path file;
  private FileHandle fileFileHandle;
  private FSDataOutputStream out;
  private FSDataInputStream fsInputStream;
  private Path dir;
  private FileHandle dirFileHandle;
  private File[] tempDirs;
  
  @Before
  public void setup() throws IOException {
    storageDir = Files.createTempDir();
    metrics = new MetricsAccumulator(new LogMetricPublisher(LOGGER), 
        TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    tempDirs = new File[2];
    tempDirs[0] = Files.createTempDir();
    tempDirs[1] = Files.createTempDir();
    fs = mock(FileSystem.class);
    fileStatus = mock(FileStatus.class);
    out = mock(FSDataOutputStream.class);
    fsInputStream = mock(FSDataInputStream.class);
    ConcurrentMap<FileHandle, HDFSFile> openFilesMap = Maps.newConcurrentMap();
    Map<FileHandle, WriteOrderHandler> writeOrderHandlerMap = Maps.newHashMap();
    FileHandleINodeMap fileHandleINodeMap = 
        new FileHandleINodeMap(new File(storageDir, "map"));
    hdfsState = new HDFSState(fileHandleINodeMap, tempDirs, fs, metrics, writeOrderHandlerMap, openFilesMap);    
    HDFSStateBackgroundWorker hdfsStateBackgroundWorker = 
        new HDFSStateBackgroundWorker(hdfsState, writeOrderHandlerMap, 
            openFilesMap, fileHandleINodeMap, 60L * 1000L, 30L, 30L* 60L * 1000L);
    hdfsStateBackgroundWorker.setDaemon(true);
    hdfsStateBackgroundWorker.start();
    stateID1 = new StateID();
    OpaqueData12 opaque = new OpaqueData12();
    opaque.setData("1".getBytes(Charsets.UTF_8));
    stateID1.setData(opaque);
    stateID2 = new StateID();
    opaque = new OpaqueData12();
    opaque.setData("2".getBytes(Charsets.UTF_8));
    stateID2.setData(opaque);
    when(fs.getFileStatus(any(Path.class))).thenReturn(fileStatus);
    file = new Path("file");
    dir = new Path("dir");
    fileFileHandle = hdfsState.getOrCreateFileHandle(file);
    when(fs.create(any(Path.class), any(Boolean.class))).thenReturn(out);
    when(fs.open(any(Path.class))).thenReturn(fsInputStream);
    dirFileHandle = hdfsState.getOrCreateFileHandle(dir);
  }
  @After
  public void teardown() throws Exception {
    if(storageDir != null) {
      PathUtils.fullyDelete(storageDir);
    }
    if(tempDirs != null) {
      for(File tempDir : tempDirs) {
        PathUtils.fullyDelete(tempDir);
      }
    }
  }
  @Test
  public void testClose() throws Exception {
    hdfsState.openForWrite(stateID1, fileFileHandle, false);
    StateID stateID = hdfsState.close("test", stateID1, 2, fileFileHandle);
    Assert.assertEquals(stateID1, stateID);
    verify(out).close();
  }
  
  @Test
  public void testCloseOldStateID() throws Exception {
    Assert.assertSame(fsInputStream, 
        hdfsState.openForRead(stateID1, fileFileHandle).getFSDataInputStream());
    try {
      hdfsState.close("test", stateID2, 2, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_OLD_STATEID, e.getError());
    }
  }
  
  @Test
  public void testCloseRead() throws Exception {
    Assert.assertSame(fsInputStream, 
        hdfsState.openForRead(stateID1, fileFileHandle).getFSDataInputStream());
    StateID stateID = hdfsState.close("test", stateID1, 2, fileFileHandle);
    Assert.assertEquals(stateID1, stateID);
    verify(fsInputStream).close();
  }
  @Test
  public void testCloseUnknownFileHandle() throws Exception {
    try {
      hdfsState.close("test", stateID1, 2, new FileHandle());
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  @Test
  public void testCloseUnknownStateID() throws Exception {
    hdfsState.openForWrite(stateID1, fileFileHandle, false);  
    try {
      hdfsState.close("test", stateID2, 2, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_FILE_OPEN, e.getError());
    }
  }
  @Test
  public void testConfirmOldStateId() throws Exception {
    Assert.assertSame(fsInputStream, 
        hdfsState.openForRead(stateID1, fileFileHandle).getFSDataInputStream());
    try {
      hdfsState.confirm(stateID2, 1, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_OLD_STATEID, e.getError());
    }
  }
  @Test
  public void testConfirmUnKnownFileHandle() throws Exception {
    try {
      hdfsState.confirm(stateID1, 1, new FileHandle());
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  @Test
  public void testConfirmWrongStateId() throws Exception {
    hdfsState.openForWrite(stateID1, fileFileHandle, false);  
    try {
      hdfsState.confirm(stateID2, 1, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_FILE_OPEN, e.getError());
    }
  }
  public void testCreateFileHandle() throws Exception {
    FileHandle fileHandle1 = hdfsState.getOrCreateFileHandle(new Path("does/not/exixt"));
    FileHandle fileHandle2 = hdfsState.getOrCreateFileHandle(new Path("does/not/exixt"));
    Assert.assertEquals(fileHandle1, fileHandle2);
  }
  @Test
  public void testDeleteNotOpenForWrite() throws Exception {
    when(fs.delete(file, false)).thenReturn(true);
    Assert.assertTrue(hdfsState.delete(file));
  }
  
  @Test
  public void testDeleteOpenForWrite() throws Exception {
    hdfsState.openForWrite(stateID1, fileFileHandle, false);
    when(fs.delete(file, false)).thenReturn(true);
    Assert.assertFalse(hdfsState.delete(file));
  }
  @Test
  public void testFileExistsDoesExist() throws Exception {
    when(fs.exists(file)).thenReturn(true);
    Assert.assertTrue(hdfsState.fileExists(file));
  }
  @Test
  public void testFileExistsDoesNotExist() throws Exception {
    Assert.assertFalse(hdfsState.fileExists(new Path("does/not/exist")));
  }

  @Test
  public void testFileExistsIsOpenForWrite() throws Exception {
    hdfsState.openForWrite(stateID1, fileFileHandle, false);
    Assert.assertTrue(hdfsState.fileExists(file));
  }
  @Test
  public void testForRead() throws Exception {
    Assert.assertSame(fsInputStream, 
        hdfsState.openForRead(stateID1, fileFileHandle).getFSDataInputStream());
    StateID stateIDConfimed = hdfsState.confirm(stateID1, 1, fileFileHandle);
    Assert.assertSame(fsInputStream,
        hdfsState.forRead(stateIDConfimed, fileFileHandle).getFSDataInputStream());
  }
  @Test
  public void testForReadFileOpenForWrite() throws Exception {
    hdfsState.openForWrite(stateID1, fileFileHandle, false);  
    try {
      hdfsState.forRead(stateID1, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_FILE_OPEN, e.getError());
    }
  }
  @Test
  public void testForReadIsDirectory() throws Exception {
    when(fileStatus.isDir()).thenReturn(true);
    try {
      hdfsState.openForRead(stateID1, dirFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_ISDIR, e.getError());
    }
  }

  @Test
  public void testForReadUncomfired() throws Exception {
    Assert.assertSame(fsInputStream, 
        hdfsState.openForRead(stateID1, fileFileHandle).getFSDataInputStream());
    try {
      hdfsState.forRead(stateID1, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_DENIED, e.getError());
    }
  }
  @Test
  public void testForReadUnknownFileHandle() throws Exception {
    try {
      hdfsState.forRead(stateID1, new FileHandle());
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  @Test
  public void testForWriteDifferentStatIDsResultsInError() throws Exception {
    @SuppressWarnings("unused")
    HDFSOutputStream out1 = hdfsState.openForWrite(stateID1, fileFileHandle, false);
    try {
      hdfsState.forWrite(stateID2, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_FILE_OPEN, e.getError());
    }
  }
  @Test
  public void testForWriteNoFileHandle() throws Exception {
    try {
      hdfsState.openForWrite(stateID1, new FileHandle(), true);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  @Test
  public void testForWriteOnDir() throws Exception {
    when(fs.exists(dir)).thenReturn(true);
    when(fileStatus.isDir()).thenReturn(true);
    try {
      hdfsState.openForWrite(stateID1, dirFileHandle, true);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_ISDIR, e.getError());
    }
  }
  @Test
  public void testForWriteOverwrite() throws Exception {    
    when(fs.exists(file)).thenReturn(true);
    try {
      hdfsState.openForWrite(stateID1, fileFileHandle, false);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_PERM, e.getError());
    }
    Assert.assertNotNull(hdfsState.openForWrite(stateID1, fileFileHandle, true));
  }
  @Test
  public void testForWriteSameStateIDIsSameOutputStream() throws Exception {
    HDFSOutputStream out1 = hdfsState.openForWrite(stateID1, fileFileHandle, false);
    Assert.assertSame(out1, hdfsState.forWrite(stateID1, fileFileHandle));
    Assert.assertSame(out1, hdfsState.forWrite(stateID1, fileFileHandle));
  }
  @Test
  public void testGetFileHandle() throws Exception {
    Assert.assertEquals(fileFileHandle, hdfsState.getFileHandle(file));
    try {
      hdfsState.getFileHandle(new Path("does/not/exixt"));
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  @Test
  public void testGetFileIDKnownFile() throws Exception {
    // FIXME cannot predict what it will be, should use di to test
    Assert.assertEquals(hdfsState.getFileID(file), hdfsState.getFileID(file));
    Assert.assertFalse(hdfsState.getFileID(file) == hdfsState.getFileID(dir));
  }
  @Test
  public void testGetFileIDUnknownFile() throws Exception {
    try {
      hdfsState.getFileID(new Path("not/a/file"));
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  @Test
  public void testGetPath() throws Exception {
    Assert.assertEquals(file, hdfsState.getPath(fileFileHandle));
    try {
      hdfsState.getPath(new FileHandle());
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  @Test
  public void testGetSizeFileOpenForWrite() throws Exception {
    when(fileStatus.getPath()).thenReturn(file);
    when(fileStatus.getLen()).thenReturn(123L);
    HDFSOutputStream out = hdfsState.openForWrite(stateID1, fileFileHandle, false);
    
    for (int i = 0; i < 532; i++) {
      out.write(Byte.MAX_VALUE);
    }
    Assert.assertEquals(532, hdfsState.getFileSize(fileStatus));
  }
  @Test
  public void testGetSizeKnownFileHandle() throws Exception {
    when(fileStatus.getPath()).thenReturn(file);
    when(fileStatus.getLen()).thenReturn(123L);
    Assert.assertEquals(123L, hdfsState.getFileSize(fileStatus));
  }
  @Test
  public void testGetSizeUnknownFileHandle() throws Exception {
    when(fileStatus.getPath()).thenReturn(new Path("does/not/exist"));
    when(fileStatus.getLen()).thenReturn(123L);
    Assert.assertEquals(123L, hdfsState.getFileSize(fileStatus));
  }
  @Test
  public void testGetWriteOrderHandler() throws Exception {
    hdfsState.openForWrite(stateID1, fileFileHandle, false);  
    WriteOrderHandler writeOrderHandler1 = hdfsState.getOrCreateWriteOrderHandler(fileFileHandle);
    WriteOrderHandler writeOrderHandler2 = hdfsState.getOrCreateWriteOrderHandler(fileFileHandle);
    Assert.assertSame(writeOrderHandler1, writeOrderHandler2);
    FileHandle fileFileHandle2 = fileFileHandle = hdfsState.getOrCreateFileHandle(new Path("file2"));;
    hdfsState.openForWrite(stateID1, fileFileHandle2, false); 
    WriteOrderHandler writeOrderHandler3 = hdfsState.getOrCreateWriteOrderHandler(fileFileHandle);
    Assert.assertNotSame(writeOrderHandler1, writeOrderHandler3);
  }
  @Test
  public void testGetWriteOrderHandlerByFileHandle() throws Exception {
    hdfsState.openForWrite(stateID1, fileFileHandle, false);  
    WriteOrderHandler writeOrderHandler1 = hdfsState.getOrCreateWriteOrderHandler(fileFileHandle);
    WriteOrderHandler writeOrderHandler2 = hdfsState.getWriteOrderHandler(fileFileHandle);
    Assert.assertSame(writeOrderHandler1, writeOrderHandler2);
  }
  @Test
  public void testGetWriteOrderHandlerNotOpenForWrite() throws Exception {
    Assert.assertNull(hdfsState.getWriteOrderHandler(fileFileHandle));
  }
  @Test
  public void testGetWriteOrderHandlerUnknownFileHandle() throws Exception {
    Assert.assertNull(hdfsState.getWriteOrderHandler(new FileHandle()));
  }
  @Test
  public void testIsFileOpenNotOpen() throws Exception {
    Assert.assertFalse(hdfsState.isFileOpen(file));
  }
  @Test
  public void testIsFileOpenRead() throws Exception {
    Assert.assertSame(fsInputStream, 
        hdfsState.openForRead(stateID1, fileFileHandle).getFSDataInputStream());
    Assert.assertTrue(hdfsState.isFileOpen(file));
  }
  @Test
  public void testIsFileOpenWrite() throws Exception {
    hdfsState.openForWrite(stateID1, fileFileHandle, false);  
    Assert.assertTrue(hdfsState.isFileOpen(file));
  }
}
