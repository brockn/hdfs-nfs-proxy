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
package com.cloudera.hadoop.hdfs.nfs.security;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class TestInitializeResponse {
  private static final byte[] EMPTY = new byte[0];
  private static final byte[] CONTEXT_ID = Bytes.toBytes(1);
  private static final byte[] TOKEN = Bytes.toBytes(2);
  @Test
  public void testSerializtion() {
    InitializeResponse response1 = new InitializeResponse();
    response1.setContextID(CONTEXT_ID);
    response1.setMajorErrorCode(10);
    response1.setMinorErrorCode(20);
    response1.setToken(TOKEN);
    
    RPCBuffer buffer = new RPCBuffer();
    response1.write(buffer);
    buffer.flip();
    response1 = new InitializeResponse();
    response1.read(buffer);
    Assert.assertTrue(Arrays.equals(CONTEXT_ID, response1.getContextID()));
    Assert.assertEquals(10, response1.getMajorErrorCode());
    Assert.assertEquals(20, response1.getMinorErrorCode());
    Assert.assertTrue(Arrays.equals(TOKEN, response1.getToken()));
  }
  @Test
  public void testNulls() {
    InitializeResponse response = new InitializeResponse();
    response.setContextID(null);
    response.setToken(null);
    Assert.assertTrue(Arrays.equals(EMPTY, response.getContextID()));
    Assert.assertTrue(Arrays.equals(EMPTY, response.getToken()));
  }
}
