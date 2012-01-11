/**
 * Copyright 2011 The Apache Software Foundation
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
import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestWritableFileFileHandleStore {

  @Test
  public void testWrite() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(NFS_FILEHANDLE_STORE_CLASS, WritableFileFileHandleStore.class, FileHandleStore.class);
    File file = new File("/tmp/" + UUID.randomUUID().toString());
    file.delete();
    conf.set(NFS_FILEHANDLE_STORE_FILE, file.getAbsolutePath());
    WritableFileFileHandleStore store = (WritableFileFileHandleStore)FileHandleStore.get(conf);
    assertTrue(store.getAll().isEmpty());
    long inode = System.currentTimeMillis();
    byte[] fileHandle = String.valueOf(inode).getBytes();
    String name = String.valueOf(inode);
    FileHandleStoreEntry entry = new FileHandleStoreEntry(fileHandle, name, inode);
    store.storeFileHandle(entry);
    assertTrue(store.getAll().size() == 1);
    entry = store.getAll().get(0);
    assertTrue(entry.fileID == inode);
    assertTrue(Arrays.equals(fileHandle, entry.fileHandle));
    assertEquals(name, entry.path);
    store.close();
    
    store = (WritableFileFileHandleStore)FileHandleStore.get(conf);
    assertTrue(store.getAll().size() == 1);
    entry = store.getAll().get(0);
    assertTrue(entry.fileID == inode);
    assertTrue(Arrays.equals(fileHandle, entry.fileHandle));
    assertEquals(name, entry.path);
    store.close();
    file.delete();
  }
}
