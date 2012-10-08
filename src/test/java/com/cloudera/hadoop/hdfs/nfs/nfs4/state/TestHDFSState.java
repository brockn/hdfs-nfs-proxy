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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_DENIED;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_FILE_OPEN;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_ISDIR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_OLD_STATEID;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_PERM;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_STALE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MemoryBackedWrite;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MemoryFileHandleStore;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Metrics;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData12;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestHDFSState {

  private Metrics metrics;
  private FileSystem fs;
  private FileStatus fileStatus;
  private HDFSState hdfsState;
  private StateID stateID1, stateID2;
  private Path file;
  private FileHandle fileFileHandle;
  private FSDataOutputStream out;
  private FSDataInputStream in;
  private Path dir;
  private FileHandle dirFileHandle;
  private String[] tempDirs;
  
  @Before
  public void setup() throws IOException {
    metrics = new Metrics();
    tempDirs = new String[1];
    tempDirs[0] = Files.createTempDir().getAbsolutePath();
    hdfsState = new HDFSState(new MemoryFileHandleStore(), tempDirs, metrics);    
    stateID1 = new StateID();
    OpaqueData12 opaque = new OpaqueData12();
    opaque.setData("1".getBytes(Charsets.UTF_8));
    stateID1.setData(opaque);
    stateID2 = new StateID();
    opaque = new OpaqueData12();
    opaque.setData("2".getBytes(Charsets.UTF_8));
    stateID2.setData(opaque);
    fs = mock(FileSystem.class);
    fileStatus = mock(FileStatus.class);
    when(fs.getFileStatus(any(Path.class))).thenReturn(fileStatus);
    out = mock(FSDataOutputStream.class);
    in = mock(FSDataInputStream.class);
    file = new Path("file");
    dir = new Path("dir");
    fileFileHandle = hdfsState.createFileHandle(file);
    when(fs.create(any(Path.class), any(Boolean.class))).thenReturn(out);
    when(fs.open(any(Path.class))).thenReturn(in);
    dirFileHandle = hdfsState.createFileHandle(dir);
  }
  @After
  public void teardown() throws Exception {
    if(tempDirs != null) {
      for(String tempDir : tempDirs) {
        PathUtils.fullyDelete(new File(tempDir));
      }
    }
  }
  @Test
  public void testDeleteNotOpenForWrite() throws Exception {
    when(fs.delete(file, false)).thenReturn(true);
    Assert.assertTrue(hdfsState.delete(fs, file));
  }
  
  @Test
  public void testDeleteOpenForWrite() throws Exception {
    hdfsState.forWrite(stateID1, fs, fileFileHandle, false);
    when(fs.delete(file, false)).thenReturn(true);
    Assert.assertFalse(hdfsState.delete(fs, file));
  }
  
  @Test
  public void testForWriteSameStateIDIsSameOutputStream() throws Exception {
    HDFSOutputStream out1 = hdfsState.forWrite(stateID1, fs, fileFileHandle, false);
    Assert.assertSame(out1, hdfsState.forWrite(stateID1, fs, fileFileHandle, false));
    Assert.assertSame(out1, hdfsState.forWrite(stateID1, fs, fileFileHandle, true));
  }
  @Test
  public void testForWriteDifferentStatIDsResultsInError() throws Exception {
    @SuppressWarnings("unused")
    HDFSOutputStream out1 = hdfsState.forWrite(stateID1, fs, fileFileHandle, false);
    try {
      hdfsState.forWrite(stateID2, fs, fileFileHandle, false);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_FILE_OPEN, e.getError());
    }
    try {
      hdfsState.forWrite(stateID2, fs, fileFileHandle, true);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_FILE_OPEN, e.getError());
    }
  }
  @Test
  public void testForWriteOverwrite() throws Exception {    
    when(fs.exists(file)).thenReturn(true);
    try {
      hdfsState.forWrite(stateID1, fs, fileFileHandle, false);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_PERM, e.getError());
    }
    Assert.assertNotNull(hdfsState.forWrite(stateID1, fs, fileFileHandle, true));
  }
  @Test
  public void testForWriteOnDir() throws Exception {
    when(fs.exists(dir)).thenReturn(true);
    when(fileStatus.isDir()).thenReturn(true);
    try {
      hdfsState.forWrite(stateID1, fs, dirFileHandle, true);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_ISDIR, e.getError());
    }
  }
  @Test
  public void testForWriteNoFileHandle() throws Exception {
    try {
      hdfsState.forWrite(stateID1, fs, new FileHandle(), true);
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
  public void testCreateFileHandle() throws Exception {
    FileHandle fileHandle1 = hdfsState.createFileHandle(new Path("does/not/exixt"));
    FileHandle fileHandle2 = hdfsState.createFileHandle(new Path("does/not/exixt"));
    Assert.assertEquals(fileHandle1, fileHandle2);
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
  public void testGetSizeKnownFileHandle() throws Exception {
    when(fileStatus.getPath()).thenReturn(file);
    when(fileStatus.getLen()).thenReturn(123L);
    Assert.assertEquals(123L, hdfsState.getFileSize(fileStatus));
  }
  @Test
  public void testGetSizeFileOpenForWrite() throws Exception {
    when(fileStatus.getPath()).thenReturn(file);
    when(fileStatus.getLen()).thenReturn(123L);
    HDFSOutputStream out = hdfsState.forWrite(stateID1, fs, fileFileHandle, false);
    
    for (int i = 0; i < 532; i++) {
      out.write(Byte.MAX_VALUE);
    }
    Assert.assertEquals(532, hdfsState.getFileSize(fileStatus));
  }
  @Test
  public void testGetSizeUnknownFileHandle() throws Exception {
    when(fileStatus.getPath()).thenReturn(new Path("does/not/exist"));
    when(fileStatus.getLen()).thenReturn(123L);
    Assert.assertEquals(123L, hdfsState.getFileSize(fileStatus));
  }
  
  @Test
  public void testForCommitFileNotOpen() throws Exception {
    try {
      hdfsState.forCommit(fs, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  
  @Test
  public void testForCommitUnknownFileHandle() throws Exception {
    try {
      hdfsState.forCommit(fs, new FileHandle());
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  
  @Test
  public void testForCommitOpenFile() throws Exception {
    HDFSOutputStream out = hdfsState.forWrite(stateID1, fs, fileFileHandle, false);  
    Assert.assertEquals(0, out.getPos());
    WriteOrderHandler writeOrderHandler1 = hdfsState.getWriteOrderHandler("test", out);
    WriteOrderHandler writeOrderHandler2 = hdfsState.forCommit(fs, fileFileHandle);
    Assert.assertSame(writeOrderHandler1, writeOrderHandler2);
    MemoryBackedWrite write = new MemoryBackedWrite("test", 1, 0, false, new byte[512], 0, 512);
    writeOrderHandler2.write(write);
    Thread.sleep(1000L);
    Assert.assertEquals(512, out.getPos());
  }
  @Test
  public void testGetWriteOrderHandler() throws Exception {
    HDFSOutputStream out1 = hdfsState.forWrite(stateID1, fs, fileFileHandle, false);  
    WriteOrderHandler writeOrderHandler1 = hdfsState.getWriteOrderHandler("test", out1);
    WriteOrderHandler writeOrderHandler2 = hdfsState.getWriteOrderHandler("test", out1);
    Assert.assertSame(writeOrderHandler1, writeOrderHandler2);
    FileHandle fileFileHandle2 = fileFileHandle = hdfsState.createFileHandle(new Path("file2"));;
    HDFSOutputStream out2 = hdfsState.forWrite(stateID1, fs, fileFileHandle2, false); 
    WriteOrderHandler writeOrderHandler3 = hdfsState.getWriteOrderHandler("test", out2);
    Assert.assertNotSame(writeOrderHandler1, writeOrderHandler3);
  }
  
  @Test
  public void testForReadUnknownFileHandle() throws Exception {
    try {
      hdfsState.forRead(stateID1, fs, new FileHandle());
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_STALE, e.getError());
    }
  }
  @Test
  public void testForReadFileOpenForWrite() throws Exception {
    hdfsState.forWrite(stateID1, fs, fileFileHandle, false);  
    try {
      hdfsState.forRead(stateID1, fs, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_FILE_OPEN, e.getError());
    }
  }
  @Test
  public void testForReadIsDirectory() throws Exception {
    when(fileStatus.isDir()).thenReturn(true);
    try {
      hdfsState.forRead(stateID1, fs, dirFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_ISDIR, e.getError());
    }
  }
  @Test
  public void testForReadUncomfired() throws Exception {
    Assert.assertSame(in, hdfsState.forRead(stateID1, fs, fileFileHandle));
    try {
      hdfsState.forRead(stateID1, fs, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_DENIED, e.getError());
    }
  }
  @Test
  public void testForRead() throws Exception {
    Assert.assertSame(in, hdfsState.forRead(stateID1, fs, fileFileHandle));
    StateID stateIDConfimed = hdfsState.confirm(stateID1, 1, fileFileHandle);
    Assert.assertSame(in, hdfsState.forRead(stateIDConfimed, fs, fileFileHandle));
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
  public void testConfirmOldStateId() throws Exception {
    try {
      hdfsState.confirm(stateID1, 1, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_OLD_STATEID, e.getError());
    }
  }
  @Test
  public void testConfirmWrongStateId() throws Exception {
    hdfsState.forWrite(stateID1, fs, fileFileHandle, false);  
    try {
      hdfsState.confirm(stateID2, 1, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_FILE_OPEN, e.getError());
    }
  }
  @Test
  public void testIsFileOpenWrite() throws Exception {
    hdfsState.forWrite(stateID1, fs, fileFileHandle, false);  
    Assert.assertTrue(hdfsState.isFileOpen(file));
  }
  @Test
  public void testIsFileOpenRead() throws Exception {
    Assert.assertSame(in, hdfsState.forRead(stateID1, fs, fileFileHandle));
    Assert.assertTrue(hdfsState.isFileOpen(file));
  }
  @Test
  public void testIsFileOpenNotOpen() throws Exception {
    Assert.assertFalse(hdfsState.isFileOpen(file));
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
    hdfsState.forWrite(stateID1, fs, fileFileHandle, false);  
    try {
      hdfsState.close("test", stateID2, 2, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_FILE_OPEN, e.getError());
    }
  }
  @Test
  public void testCloseOldStateID() throws Exception {
    try {
      hdfsState.close("test", stateID2, 2, fileFileHandle);
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_OLD_STATEID, e.getError());
    }
  }
  @Test
  public void testCloseRead() throws Exception {
    Assert.assertSame(in, hdfsState.forRead(stateID1, fs, fileFileHandle));
    StateID stateID = hdfsState.close("test", stateID1, 2, fileFileHandle);
    Assert.assertEquals(stateID1, stateID);
    verify(in).close();
  }
  @Test
  public void testClose() throws Exception {
    hdfsState.forWrite(stateID1, fs, fileFileHandle, false);
    StateID stateID = hdfsState.close("test", stateID1, 2, fileFileHandle);
    Assert.assertEquals(stateID1, stateID);
    verify(out).close();
  }
  @Test
  public void testCloseWouldBlockUnknownFileHandle() throws Exception {
    Assert.assertFalse(hdfsState.closeWouldBlock(new FileHandle()));
  }
  @Test
  public void testCloseWouldBlockFileOpenForRead() throws Exception {
    Assert.assertSame(in, hdfsState.forRead(stateID1, fs, fileFileHandle));
    Assert.assertFalse(hdfsState.closeWouldBlock(fileFileHandle));
  }
  @Test
  public void testCloseWouldBlockFileNotOpen() throws Exception {
    Assert.assertFalse(hdfsState.closeWouldBlock(fileFileHandle));
  }
  @Test
  public void testCloseWouldBlockShouldNotBlock() throws Exception {
    HDFSOutputStream out = hdfsState.forWrite(stateID1, fs, fileFileHandle, false);
    WriteOrderHandler writeOrderHandler = hdfsState.getWriteOrderHandler("test", out);
    MemoryBackedWrite write = new MemoryBackedWrite("test", 1, 0, false, new byte[512], 0, 512);
    writeOrderHandler.write(write);
    Assert.assertFalse(hdfsState.closeWouldBlock(fileFileHandle));
  }
  @Test
  public void testCloseWouldBlockShouldBlock() throws Exception {
    HDFSOutputStream out = hdfsState.forWrite(stateID1, fs, fileFileHandle, false);
    WriteOrderHandler writeOrderHandler = hdfsState.getWriteOrderHandler("test", out);
    MemoryBackedWrite write = new MemoryBackedWrite("test", 1, 1, false, new byte[512], 0, 512);
    writeOrderHandler.write(write);
    Assert.assertTrue(hdfsState.closeWouldBlock(fileFileHandle));
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
  public void testFileExistsDoesNotExist() throws Exception {
    Assert.assertFalse(hdfsState.fileExists(fs, new Path("does/not/exist")));
  }
  @Test
  public void testFileExistsDoesExist() throws Exception {
    when(fs.exists(file)).thenReturn(true);
    Assert.assertTrue(hdfsState.fileExists(fs, file));
  }
  @Test
  public void testFileExistsIsOpenForWrite() throws Exception {
    hdfsState.forWrite(stateID1, fs, fileFileHandle, false);
    Assert.assertTrue(hdfsState.fileExists(fs, file));
  }
}
