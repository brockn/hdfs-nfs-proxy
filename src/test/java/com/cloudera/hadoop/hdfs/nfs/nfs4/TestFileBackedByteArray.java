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

import java.io.File;
import java.io.RandomAccessFile;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestFileBackedByteArray {
  private File baseDir;
  private FileBackedByteArray array;
  private File file;
  private RandomAccessFile randomAccessFile;
  private byte[] data;

  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
    file = new File(baseDir, "test1");
    randomAccessFile = new RandomAccessFile(file, "rw");
    data = "data".getBytes(Charsets.UTF_8);
  }
  @After
  public void teardown() throws Exception {
    randomAccessFile.close();
    PathUtils.fullyDelete(baseDir);
  }
  @Test
  public void testBasic() throws Exception {
    array = FileBackedByteArray.create(file, randomAccessFile,
        data, 0, data.length);
    byte[] actual = array.getByteArray();
    Assert.assertArrayEquals(data, actual);
    Assert.assertEquals(0L, array.getOffset());
    Assert.assertEquals(file, array.getFile());
    Assert.assertEquals(data.length, array.getLength());
    Assert.assertEquals(Bytes.hashBytes(data, 0, data.length), array.getArrayHashCode());
  }

  @Test(expected=IllegalStateException.class)
  public void testBadLength() throws Exception {
    array = FileBackedByteArray.create(file, randomAccessFile,
        data, 0, data.length);
    RandomAccessFile fileHandle = new RandomAccessFile(file, "rw");
    fileHandle.seek(0L);
    fileHandle.writeInt(-1);
    fileHandle.close();
    array.getByteArray();
  }
}
