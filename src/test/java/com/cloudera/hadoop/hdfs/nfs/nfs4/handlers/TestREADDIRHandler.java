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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.DirectoryEntry;
import com.cloudera.hadoop.hdfs.nfs.nfs4.DirectoryList;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.READDIRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.READDIRResponse;

public class TestREADDIRHandler extends TestBaseHandler {

  private READDIRHandler handler;
  private READDIRRequest request;
  
  private Path dir;

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new READDIRHandler();
    request = new READDIRRequest();
    dir = new Path("/somedir");    
    request.setCookieVerifer(new OpaqueData8(0L));
    request.setCookie(0L);
    request.setMaxCount(1024);
    request.setAttrs(new Bitmap());
    when(hdfsState.getPath(currentFileHandle)).thenReturn(dir);
    when(fs.getFileStatus(dir)).thenReturn(isdir);
    FileStatus[] stati = new FileStatus[4];
    for (int index = 0; index < stati.length; index++) {
      stati[index] = mock(FileStatus.class);
      when(stati[index].getPath()).thenReturn(new Path(dir, String.valueOf(index)));
    }
    when(fs.listStatus(dir)).thenReturn(stati);
  }

  @Test
  public void testBadCookie() throws Exception {
    request.setCookie(Long.MAX_VALUE);
    READDIRResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_BAD_COOKIE, response.getStatus());
  }
  @Test
  public void testBadCookieHigh() throws Exception {
    request.setCookie(Long.MAX_VALUE);
    READDIRResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_BAD_COOKIE, response.getStatus());
  }
  @Test
  public void testBadCookieLow() throws Exception {
    request.setCookie(99);
    READDIRResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_BAD_COOKIE, response.getStatus());
  }
  @Test
  public void testBadVerifer() throws Exception {
    request.setCookieVerifer(new OpaqueData8(99));
    READDIRResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOT_SAME, response.getStatus());
  }
  @Test
  public void testBasicList() throws Exception {
    READDIRResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    DirectoryList directoryList = response.getDirectoryList();
    List<DirectoryEntry> entries = new ArrayList<DirectoryEntry>(directoryList.getDirEntries());
    assertEquals("0", entries.get(0).getName());
    assertEquals("1", entries.get(1).getName());
    assertEquals("2", entries.get(2).getName());
    assertEquals("3", entries.get(3).getName());
  }
  @Test
  public void testDirDoesNotExist() throws Exception {
    when(fs.getFileStatus(dir)).thenReturn(notdir);
    READDIRResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOTDIR, response.getStatus());
  }
  @Test
  public void testListRequiringMultipleRequests() throws Exception {
    FileStatus[] stati = new FileStatus[1000];
    for (int index = 0; index < stati.length; index++) {
      stati[index] = mock(FileStatus.class);
      when(stati[index].getPath()).thenReturn(new Path(dir, String.valueOf(index)));
    }
    when(fs.listStatus(dir)).thenReturn(stati);
    List<DirectoryEntry> entries = new ArrayList<DirectoryEntry>();
    boolean eof = false;
    long cookie = 0L;
    int numRequests = 0;
    while(!eof) {
      numRequests++;
      request.setCookieVerifer(new OpaqueData8(cookie));
      READDIRResponse response = handler.handle(hdfsState, session, request);
      assertEquals(NFS4_OK, response.getStatus());
      DirectoryList directoryList = response.getDirectoryList();
      eof = directoryList.isEOF();
      entries.addAll(directoryList.getDirEntries());
      request.setCookieVerifer(response.getCookieVerifer());
      for(DirectoryEntry entry : directoryList.getDirEntries()) {
        request.setCookie(entry.getCookie());     
      }
    }
    assertTrue(numRequests > 1);
    for (int index = 0; index < stati.length; index++) {
      assertEquals(stati[index].getPath().getName(), entries.get(index).getName());
    }
  }
  @Test
  public void testListReturnsNull() throws Exception {
    when(fs.listStatus(dir)).thenReturn(null);
    READDIRResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
  }
  @Test
  public void testMessageSizeTooSmall() throws Exception {
    request.setMaxCount(0);
    READDIRResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_TOOSMALL, response.getStatus());
  }
}
