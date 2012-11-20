/**
 * Copyright 2012 Cloudera Inc.
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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestFileBackedWrite {
  private File baseDir;
  private FileBackedByteArray array;
  private File backingFile1;
  private File backingFile2;
  private RandomAccessFile randomAccessFile1;
  private RandomAccessFile randomAccessFile2;
  private FileBackedWrite write;
  private int xid;
  private int offset;
  private boolean sync;
  private byte[] data;


  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
    backingFile1 = new File(baseDir, "test1");
    backingFile2 = new File(baseDir, "test2");
    randomAccessFile1 = new RandomAccessFile(backingFile1, "rw");
    randomAccessFile2 = new RandomAccessFile(backingFile2, "rw");
    xid = 1;
    offset = 0;
    sync = false;
    data = "data".getBytes(Charsets.UTF_8);
    array = FileBackedByteArray.create(backingFile1, randomAccessFile1, data, 0, data.length);
    write = new FileBackedWrite(array, xid, offset, sync);
  }
  @After
  public void teardown() throws Exception {
    randomAccessFile1.close();
    randomAccessFile2.close();
    PathUtils.fullyDelete(baseDir);
  }
  @Test
  public void testBasic() throws Exception {
    assertEquals(xid, write.getXid());
    assertEquals("1", write.getXidAsHexString());
    assertEquals(offset, write.getOffset());
    assertEquals(sync, write.isSync());
    assertEquals(3999531, write.hashCode());
    assertArrayEquals(data, write.getData());
    assertEquals(data.length, write.getLength());
    assertTrue(write.equals(write));
    assertNotNull(write.toString());
    assertEquals(0, write.getStart());
    // same
    assertTrue(write.equals(new FileBackedWrite(
        FileBackedByteArray.create(backingFile2, randomAccessFile2, data, 0, data.length), xid, offset, sync)));
    // length differs
    assertFalse(write.equals(new FileBackedWrite(
        FileBackedByteArray.create(backingFile2, randomAccessFile2, data, 0, data.length -1), xid, offset, sync)));
    // offset differs
    assertFalse(write.equals(new FileBackedWrite(
        FileBackedByteArray.create(backingFile2, randomAccessFile2, data, 0, data.length -1), xid, offset + 1, sync)));
    // data differs
    data[0] = (byte) (data[0] + 1);
    assertFalse(write.equals(new FileBackedWrite(
        FileBackedByteArray.create(backingFile2, randomAccessFile2, data, 0, data.length), xid, offset, sync)));
    assertFalse(write.equals(null));
    assertFalse(write.equals(new Object()));
  }
  @Test
  public void testRead() throws Exception {
    array = mock(FileBackedByteArray.class);
    when(array.getByteArray()).thenThrow(new IOException());
    write = new FileBackedWrite(array, xid, offset, sync);
    try {
      write.getData();
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
    }
    verify(array, times(2)).getByteArray();

  }
}
