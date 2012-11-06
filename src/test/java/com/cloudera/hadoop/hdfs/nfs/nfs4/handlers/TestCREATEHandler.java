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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CREATERequest;
import com.google.common.collect.ImmutableList;

public class TestCREATEHandler extends TestBaseHandler {

  private CREATEHandler handler;
  private CREATERequest request;
  private Path parent;
  private Path child;

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new CREATEHandler();
    request = new CREATERequest();
    request.setType(NFS4_DIR);
    request.setAttrs(new Bitmap());
    ImmutableList<Attribute> attrValues = ImmutableList.of();
    request.setAttrValues(attrValues);
    parent = new Path("a");
    child = new Path(parent, "b");
    request.setName(child.getName());
    
    when(hdfsState.getPath(currentFileHandle)).thenReturn(parent);
    when(fs.exists(parent)).thenReturn(true);
    when(fs.mkdirs(child)).thenReturn(true);
  }
  @Test
  public void testAlreadyExists() throws Exception {
    when(fs.exists(child)).thenReturn(true);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_EXIST, response.getStatus());
  }
  @Test
  public void testCreateNotDir() throws Exception {
    request.setType(NFS4_REG);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_NOTSUPP, response.getStatus());
  }
  @Test
  public void testInvalidNameEmpty() throws Exception {
    request.setName("");
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
  }
  @Test
  public void testInvalidNameNull() throws Exception {
    request.setName(null);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_INVAL, response.getStatus());
  }
  @Test
  public void testMkdirFails() throws Exception {
    when(fs.mkdirs(child)).thenReturn(false);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_IO, response.getStatus());
  }
  @Test
  public void testParentDoesNotExist() throws Exception {
    when(fs.exists(parent)).thenReturn(false);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_STALE, response.getStatus());
  }
  @Test
  public void testSuccess() throws Exception {
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
  }
}
