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
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestPendingWriteFactory {

  private PendingWriteFactory pendingWriteFactory;
  private byte[] data;
  private MemoryBackedWrite memoryWrite;

  private File baseDir;
  private FileBackedWrite fileWrite1;
  private FileBackedWrite fileWrite2;


  @Before
  public void setup() throws Exception {
    StringBuilder buffer = new StringBuilder();
    while(buffer.length() < (1025L * 64L)) {
      buffer.append("data");
    }
    data = buffer.toString().getBytes(Charsets.UTF_8);
    baseDir = Files.createTempDir();
    pendingWriteFactory = new PendingWriteFactory(new File[] { baseDir }, ONE_MB);

  }
  @After
  public void teardown() throws Exception {
    PathUtils.fullyDelete(baseDir);
  }

  @Test
  public void testBasic() throws Exception {
    memoryWrite = (MemoryBackedWrite) pendingWriteFactory.create(0L, 1, 0L, false, data, 0, data.length);
    fileWrite1 = (FileBackedWrite) pendingWriteFactory.create(0L, 1, ONE_GB, false, data, 0, data.length);
    fileWrite2 = (FileBackedWrite) pendingWriteFactory.create(0L, 1, ONE_GB, false, data, 0, data.length);
    File file = fileWrite1.getFileBackedByteArray().getFile();
    Assert.assertTrue(file.isFile()); // file refcount = 2
    pendingWriteFactory.destroy(fileWrite1);
    Assert.assertTrue(file.isFile()); // file should have refcount = 1
    pendingWriteFactory.destroy(fileWrite2);
    Assert.assertFalse(file.isFile()); // file should have refcount = 0
    pendingWriteFactory.destroy(memoryWrite);
  }

  @Test
  public void testRoll() throws Exception {
    Set<File> files = Sets.newHashSet();
    List<FileBackedWrite> writes = Lists.newArrayList();
    for (int offset = 0; offset < (ONE_MB * 3L); offset += data.length) {
      FileBackedWrite fileWrite = (FileBackedWrite) pendingWriteFactory.
          create(0L, 1, ONE_GB + offset, false, data, 0, data.length);
      writes.add(fileWrite);
    }
    for(FileBackedWrite fileWrite : writes) {
      File file = fileWrite.getFileBackedByteArray().getFile();
      files.add(file);
      Assert.assertTrue(file.isFile());
      Assert.assertArrayEquals(data, fileWrite.getData());
      pendingWriteFactory.destroy(fileWrite);
    }
    Assert.assertEquals(3, files.size());
    for(File file : files) {
      Assert.assertFalse(file.isFile());
    }
  }
}
