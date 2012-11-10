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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestFileHandleINodeMap {

  private final FileHandle fh1 = new FileHandle("fh1".getBytes(Charsets.UTF_8));
  private final FileHandle fh2 = new FileHandle("fh2".getBytes(Charsets.UTF_8));
  private final INode inode1 = new INode("/1", 1);
  private final INode inode2 = new INode("/2", 1);
  private File baseDir;
  private File file;
  private FileHandleINodeMap fileHandleINodeMap;
  
  @Before
  public void setup() throws IOException {
    baseDir = Files.createTempDir();
    file = new File(baseDir, "map");
    fileHandleINodeMap = new FileHandleINodeMap(file);
  }

  @After
  public void teardown() throws Exception {
    PathUtils.fullyDelete(baseDir);
  }
  
  @Test
  public void testLookup() throws Exception {
    fileHandleINodeMap.put(fh1, inode1);
    fileHandleINodeMap.put(fh2, inode2);
    Assert.assertEquals(inode1, fileHandleINodeMap.getINodeByFileHandle(fh1));
    Assert.assertEquals(inode2, fileHandleINodeMap.getINodeByFileHandle(fh2));
    Assert.assertEquals(fh1, fileHandleINodeMap.getFileHandleByPath(inode1.getPath()));
    Assert.assertEquals(fh2, fileHandleINodeMap.getFileHandleByPath(inode2.getPath()));
    
    fileHandleINodeMap.close();
    fileHandleINodeMap = new FileHandleINodeMap(file);
    
    Assert.assertEquals(inode1, fileHandleINodeMap.getINodeByFileHandle(fh1));
    Assert.assertEquals(inode2, fileHandleINodeMap.getINodeByFileHandle(fh2));
    Assert.assertEquals(fh1, fileHandleINodeMap.getFileHandleByPath(inode1.getPath()));
    Assert.assertEquals(fh2, fileHandleINodeMap.getFileHandleByPath(inode2.getPath()));    
  }
  @Test
  public void testNullWhenEmpty() throws Exception {
    Assert.assertNull(fileHandleINodeMap.getINodeByFileHandle(fh1));
    Assert.assertNull(fileHandleINodeMap.getFileHandleByPath(inode1.getPath()));
  }
  
  @Test
  public void testRemove() throws Exception {
    fileHandleINodeMap.put(fh1, inode1);
    Assert.assertNotNull(fileHandleINodeMap.getINodeByFileHandle(fh1));
    fileHandleINodeMap.remove(fh1);
    
    fileHandleINodeMap.close();
    fileHandleINodeMap = new FileHandleINodeMap(file);
    
    Assert.assertNull(fileHandleINodeMap.getINodeByFileHandle(fh1));
    Assert.assertNull(fileHandleINodeMap.getFileHandleByPath(inode1.getPath()));
    Assert.assertTrue(fileHandleINodeMap.getFileHandlesNotValidatedSince(0L).size() == 0);
  }
  @Test
  public void testValidationTime() throws Exception {
    fileHandleINodeMap.put(fh1, inode1);
    long time = System.currentTimeMillis() - 1L;
    Thread.sleep(500L);
    
    fileHandleINodeMap.close();
    fileHandleINodeMap = new FileHandleINodeMap(file);
    
    Map<FileHandle, String> oldFileHandles = fileHandleINodeMap.getFileHandlesNotValidatedSince(time);
    Assert.assertTrue(oldFileHandles.size() == 1);
    Assert.assertEquals(inode1.getPath(), oldFileHandles.get(fh1));
    Set<FileHandle> stillActiveFileHandles = Sets.newHashSet(oldFileHandles.keySet());
    fileHandleINodeMap.updateValidationTime(stillActiveFileHandles);
    Assert.assertTrue(fileHandleINodeMap.getINodeByFileHandle(fh1).getCreationTime() > inode1.getCreationTime());
  }
}
