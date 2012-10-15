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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestFileBackedWrite {
  private File baseDir;
  private File backingFile1;
  private File backingFile2;
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
    xid = 1;
    offset = 0;
    sync = false;
    data = "data".getBytes(Charsets.UTF_8);
    write = new FileBackedWrite(backingFile1, "test1", xid, offset, sync, data, 0, data.length);
  }
  @After
  public void teardown() throws Exception {
    PathUtils.fullyDelete(baseDir);
  }
  @Test
  public void testBasic() throws Exception {
    assertEquals(xid, write.getXid());
    assertEquals("1", write.getXidAsHexString());
    assertEquals(offset, write.getOffset());
    assertEquals(sync, write.isSync());
    assertEquals(46, write.getSize());
    assertEquals(4000492, write.hashCode());
    assertArrayEquals(data, write.getData());
    assertTrue(write.equals(write));
    assertFalse(write.equals(new FileBackedWrite(backingFile2, "test2", xid, offset, sync, data, 0, data.length - 1)));
    write.close();
    assertFalse(backingFile1.exists());
  }
}
