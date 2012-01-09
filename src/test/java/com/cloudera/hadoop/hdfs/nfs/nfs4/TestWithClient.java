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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestWithClient {
  
  
  @Test
  public void testNOENTLocal() throws IOException, InterruptedException, NFS4Exception {
    testNOENT(new LocalClient());
  }

  @Test
  public void testNOENTNetwork() throws IOException, InterruptedException, NFS4Exception {
    testNOENT(new NetworkClient());
  }

  public void testNOENT(BaseClient client) throws IOException, InterruptedException, NFS4Exception {
    try {
      client.listPath(new Path("/" + UUID.randomUUID().toString()));      
      fail("Expected no such file or directory");
    } catch (NFS4Exception e) {
      assertEquals(NFS4ERR_NOENT, e.getError());
    } finally {
      client.shutdown();
    }
  }
  
  @Test
  public void testReadDirLocal() throws IOException, InterruptedException, NFS4Exception {
    doReadDir(new LocalClient());
  }

  @Test
  public void testReadDirNetwork() throws IOException, InterruptedException, NFS4Exception {
    doReadDir(new NetworkClient());
  }

  public void doReadDir(BaseClient client) throws IOException, InterruptedException, NFS4Exception {
    /*
     * traverse through a directory that does not change often
     * and ensure it checks out the same as through the native api
     */
    Path rootPath = new Path("/");
    Path etcPath = new Path("etc/init.d");
    compareFileStatusFile(client.getFileStatus(rootPath));
    ImmutableList<Path> paths = client.listPath(new Path(rootPath, etcPath));
    File etcFile = new File(rootPath.toString(), etcPath.toString());
    assertEquals(etcFile.list().length, paths.size());
    for (Path path : paths) {
      compareFileStatusFile(client.getFileStatus(path));
    }
    
    client.shutdown();
  }
  
  @Test
  public void testSmallReadSizeLocal() throws Exception {
    doSmallReadSize(new LocalClient());
  }
  
  @Test
  public void testSmallReadSizeNetwork() throws Exception {
    doSmallReadSize(new NetworkClient());
  }
  
  public void doSmallReadSize(BaseClient client) throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(client.forRead(new Path("/etc/passwd"), 5)));
    String line;
    
    boolean foundRoot = false;
    while((line = reader.readLine()) != null) {
      if(line.startsWith("root")) {
        foundRoot = true;
        // don't break on purpose (more requests)
      }
    }  
    reader.close();
    client.shutdown();
    assertTrue(foundRoot);
  }
  
  @Test
  public void testLargeReadSizeLocal() throws Exception {
    doLargeReadSize(new LocalClient());
  }
  
  @Test
  public void testLargeReadSizeNetwork() throws Exception {
    doLargeReadSize(new NetworkClient());
  }
  
  public void doLargeReadSize(BaseClient client) throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(client.forRead(new Path("/etc/passwd"), 1024 * 1024)));
    String line;
    
    boolean foundRoot = false;
    while((line = reader.readLine()) != null) {
      if(line.startsWith("root")) {
        foundRoot = true;
        // don't break on purpose (more requests)
      }
    }  
    reader.close();
    client.shutdown();
    assertTrue(foundRoot);
  }

  @Test
  public void testNormalReadSizeLocal() throws Exception {
    doNormalReadSize(new LocalClient());
  }
  
  @Test
  public void testNormalReadSizeNetwork() throws Exception {
    doNormalReadSize(new NetworkClient());
  }
  public void doNormalReadSize(BaseClient client) throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(client.forRead(new Path("/etc/passwd"), NFS4_MAX_RWSIZE)));
    String line;
    
    boolean foundRoot = false;
    while((line = reader.readLine()) != null) {
      if(line.startsWith("root")) {
        foundRoot = true;
        // don't break on purpose (more requests)
      }
    }  
    reader.close();
    client.shutdown();
    assertTrue(foundRoot);
  }
  
  @Test
  public void testWriteLocal() throws Exception {
    doWrite(new LocalClient());
  }
  
  @Test
  public void testWriteNetwork() throws Exception {
    doWrite(new NetworkClient());
  }
  
  public void doWrite(BaseClient client) throws Exception {
    File file = new File("/tmp", UUID.randomUUID().toString());
    Path path = new Path(file.getAbsolutePath());
    try {
      OutputStream out = client.forWrite(path);
      for (int i = 0; i < 1000; i++) {
        out.write((byte)'A');
      }
      out.close();
      InputStream in = client.forRead(path, NFS4_MAX_RWSIZE);
      int b = in.read();
      while(b >= 0) {
        assertTrue("Byte = '" + String.valueOf((char)b) + "'", (char)b == 'A');
        b = in.read();
      }
      in.close();
    } finally {
      file.delete();
      client.shutdown();
    }
  }
  
  protected String getOwner(File file) throws IOException {
    String[] cmd =  new String[]{"stat","-c","%U",file.getAbsolutePath()};
    return Shell.execCommand(cmd).trim();
  }
  protected String getOwnerGroup(File file) throws IOException {
    String[] cmd =  new String[]{"/usr/bin/stat","-c","%G", file.getAbsolutePath()};
    return Shell.execCommand(cmd).trim();
  }
  protected void doCompareFileStatusFile(FileStatus fileStatus) throws IOException {
    File file = new File(fileStatus.path.toString());
    assertEquals(getOwner(file) + "@localdomain", fileStatus.getOwner());
    assertEquals(getOwnerGroup(file) + "@localdomain", fileStatus.getOwnerGroup());
    assertEquals(file.length(), fileStatus.getSize());
    assertEquals(file.lastModified(), fileStatus.getMTime());      
    assertEquals(file.isDirectory(), fileStatus.isDir());
  }
  protected void compareFileStatusFile(FileStatus fileStatus) throws IOException, InterruptedException {
    try {
      doCompareFileStatusFile(fileStatus);
    } catch(AssertionError error) {
      Thread.sleep(100L);
      doCompareFileStatusFile(fileStatus);
    }
  }

}
