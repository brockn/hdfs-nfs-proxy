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

import static org.mockito.Mockito.*;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData12;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.google.common.base.Charsets;

public class TestHDFSFile {

  private HDFSFile hdfsFile;
  private FileHandle fileHandle;
  private String path;
  private long fileID;
  
  private StateID stateID;
  
  private HDFSOutputStream out;
  private HDFSInputStream in;
  
  @Before
  public void setup() throws Exception {
    fileHandle = new FileHandle(UUID.randomUUID().toString().getBytes(Charsets.UTF_8));
    path = "/file";
    fileID = 1;
    hdfsFile = new HDFSFile(fileHandle, path, fileID);
    
    stateID = new StateID();
    OpaqueData12 opaque = new OpaqueData12();
    opaque.setData("1".getBytes(Charsets.UTF_8));
    stateID.setData(opaque);
    
    out = mock(HDFSOutputStream.class);
    in = mock(HDFSInputStream.class);
  }
  
  @Test
  public void testGetters() throws Exception {
    Assert.assertNull(hdfsFile.getHDFSOutputStream());
    Assert.assertNull(hdfsFile.getHDFSOutputStreamForWrite());
    Assert.assertNull(hdfsFile.getInputStream(new StateID()));
    hdfsFile.toString();
  }
  @Test
  public void testIsOpen() throws Exception {
    Assert.assertFalse(hdfsFile.isOpen());
    Assert.assertFalse(hdfsFile.isOpenForRead());
    Assert.assertFalse(hdfsFile.isOpenForWrite());
  }
  
  @Test
  public void testOutputStream() throws Exception {
    hdfsFile.setHDFSOutputStream(stateID, out);
    Assert.assertSame(out, hdfsFile.getHDFSOutputStream().get());
    Assert.assertSame(out, hdfsFile.getHDFSOutputStreamForWrite().get());
    Assert.assertTrue(hdfsFile.isOpen());
    Assert.assertTrue(hdfsFile.isOpenForWrite());
    Assert.assertFalse(hdfsFile.isOpenForRead());
    hdfsFile.closeOutputStream(stateID);
    verify(out).close();
  }
  
  @Test
  public void testInputStream() throws Exception {
    hdfsFile.putInputStream(stateID, in);
    Assert.assertSame(in, hdfsFile.getInputStream(stateID).get());
    Assert.assertTrue(hdfsFile.isOpen());
    Assert.assertTrue(hdfsFile.isOpenForRead());
    Assert.assertFalse(hdfsFile.isOpenForWrite());
    hdfsFile.closeInputStream(stateID);
    verify(in).close();
  }
  
  @Test
  public void testTimestamp() throws Exception {
    hdfsFile.putInputStream(stateID, in);
    hdfsFile.setHDFSOutputStream(stateID, out);
    long inputLastUsedBefore = hdfsFile.getInputStream(stateID).getTimestamp();
    long outputLastUsedBefore = hdfsFile.getHDFSOutputStreamForWrite().getTimestamp();
    Thread.sleep(1001L);
    long inputLastUsedAfter = hdfsFile.getInputStream(stateID).getTimestamp();
    long outputLastUsedAfter = hdfsFile.getHDFSOutputStreamForWrite().getTimestamp();
    Assert.assertTrue(inputLastUsedAfter - inputLastUsedBefore >= 1000);
    Assert.assertTrue(outputLastUsedAfter - outputLastUsedBefore >= 1000);
  }
}
