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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.LOCALHOST;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_NOENT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_MAX_RWSIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.NFSUtils;
import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

public class TestWithClient {

  protected static final Logger LOGGER = Logger.getLogger(TestWithClient.class);
  private File testFile1;
  private File testFile2;
  private final static String className = TestWithClient.class.getName();

  @Before
  public void setup() throws IOException {
    TestUtils.buildTempDirDataFiles(6, TestUtils.tmpDirPathForTest);
    testFile1 = TestUtils.createTempFile(null, true, 1024, className, null);
    testFile2 = TestUtils.createTempFile(null, true, 1024, className, null);
  }

  @After
  public void teardown() {
    testFile1.delete();
    testFile2.delete();
  }

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
     * traverse through a directory that does not change often and ensure it
     * checks out the same as through the native api
     */
    Path rootPath = new Path("/");
    Path etcPath = new Path(TestUtils.tmpDirPathForTest);
    compareFileStatusFile(client.getFileStatus(rootPath));
    ImmutableList<Path> paths = client.listPath(new Path(rootPath, etcPath));
    File etcFile = new File(rootPath.toString(), etcPath.toString());
    assertEquals(etcFile.list().length, paths.size());
    for (Path path : paths) {
      LOGGER.debug("checking file => " + path.getName());
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
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(
            client.forRead(new Path(testFile1.getAbsolutePath()), 5)));
    int count = 0;
    while (reader.read() != -1) {
      count++;
    }
    reader.close();
    client.shutdown();
    assertTrue(count > 0);
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
    BufferedReader reader = new BufferedReader(new InputStreamReader(client.forRead(new Path(testFile1.getCanonicalPath()), 1024 * 1024)));
    int count = 0;
    while (reader.read() != -1) {
      count++;
    }
    reader.close();
    client.shutdown();
    assertTrue(count > 0);
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
    BufferedReader reader = new BufferedReader(new InputStreamReader(client.forRead(new Path(testFile1.getCanonicalPath()), NFS4_MAX_RWSIZE)));
    int count = 0;
    while (reader.read() != -1) {
      count++;
    }
    reader.close();
    client.shutdown();
    assertTrue(count > 0);
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
        out.write((byte) 'A');
      }
      out.close();
      InputStream in = client.forRead(path, NFS4_MAX_RWSIZE);
      int b = in.read();
      while (b >= 0) {
        assertTrue("Byte = '" + String.valueOf((char) b) + "'", (char) b == 'A');
        b = in.read();
      }
      in.close();
    } finally {
      file.delete();
      client.shutdown();
    }
  }

  protected String getOwner(File file) throws IOException {
    File script = null;
    String[] cmd = new String[]{"/usr/bin/stat", "-c", "%U", file.getCanonicalPath()};
    if("Mac OS X".equalsIgnoreCase(System.getProperty("os.name"))) {
      script = File.createTempFile("stat", ".sh");
      String statCmd = "stat " + file.getCanonicalPath() + "| cut -d ' ' -f5";
      cmd = new String[2];
      cmd[0] = "bash";
      cmd[1] = script.getAbsolutePath();
      Files.write(statCmd, script, Charsets.UTF_8);
    }
    LOGGER.info("Executing " + Arrays.toString(cmd));
    try {
      return Shell.execCommand(cmd).trim();      
    } finally {
      if(script != null) {
        script.delete();        
      }
    }
  }

  protected String getOwnerGroup(File file) throws IOException {
    File script = null;
    String[] cmd = new String[]{"/usr/bin/stat", "-c", "%G", file.getAbsolutePath()};
    if("Mac OS X".equalsIgnoreCase(System.getProperty("os.name"))) {
      script = File.createTempFile("stat", ".sh");
      String statCmd = "stat " + file.getCanonicalPath() + "| cut -d ' ' -f6";
      cmd = new String[2];
      cmd[0] = "bash";
      cmd[1] = script.getAbsolutePath();
      Files.write(statCmd, script, Charsets.UTF_8);
    }
    LOGGER.info("Executing " + Arrays.toString(cmd));
    try {
      return Shell.execCommand(cmd).trim();      
    } finally {
      if(script != null) {
        script.delete();        
      }
    }
  }

  protected void doCompareFileStatusFile(FileStatus fileStatus) throws IOException {
    File file = new File(fileStatus.path.toString());
    String domain = NFSUtils.getDomain(new Configuration(), LOCALHOST);
    assertEquals(getOwner(file) + "@" + domain, fileStatus.getOwner());
    assertEquals(getOwnerGroup(file) + "@" + domain, fileStatus.getOwnerGroup());
    assertEquals(file.length(), fileStatus.getSize());
    assertEquals(file.lastModified(), fileStatus.getMTime());
    assertEquals(file.isDirectory(), fileStatus.isDir());
  }

  protected void compareFileStatusFile(FileStatus fileStatus) throws IOException, InterruptedException {
    try {
      doCompareFileStatusFile(fileStatus);
    } catch (AssertionError error) {
      Thread.sleep(100L);
      doCompareFileStatusFile(fileStatus);
    }
  }
}
