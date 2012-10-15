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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestFIleHandleStoreEntry {

  private FileHandleStoreEntry entry;
  private byte[] fileHandle;
  private String path;
  private long fileID;
  
  @Before
  public void setup() throws Exception {
    fileHandle = "fileHandle".getBytes(Charsets.UTF_8);
    path = "/a";
    fileID = 1;
    entry = new FileHandleStoreEntry(fileHandle, path, fileID);
  }
  @Test
  public void testGetters() throws Exception {
    assertArrayEquals(fileHandle, entry.getFileHandle());
    assertEquals(path, entry.getPath());
    assertEquals(fileID, entry.getFileID());
  }
  @Test
  public void testSerialize() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOutput = new DataOutputStream(out);
    entry.write(dataOutput);
    dataOutput.flush();
    dataOutput.close();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    FileHandleStoreEntry copy = new FileHandleStoreEntry();
    copy.readFields(in);
    assertEquals(entry, copy);
    assertEquals(entry.hashCode(), copy.hashCode());
  }
  @Test
  public void testCompareTo() throws Exception {
    FileHandleStoreEntry other = new FileHandleStoreEntry(fileHandle, "/b", fileID);
    assertTrue(entry.compareTo(other) < 0);
    assertTrue(other.compareTo(entry) > 0);
    assertTrue(entry.compareTo(entry) == 0);
  }
}
