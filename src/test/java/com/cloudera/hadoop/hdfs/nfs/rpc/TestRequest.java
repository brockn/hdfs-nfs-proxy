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
package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.TestUtils.*;

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.OperationFactory;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OperationRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.Lists;

public class TestRequest {

  @Test
  public void testRequestWire() throws Exception {
    OperationFactory.register(TEST_OPERATION_ID, OperationTest.class);
    CompoundRequest base = new CompoundRequest();
    base.setMinorVersion(1);
    List<OperationRequest> ops = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      ops.add(new OperationTest(UUID.randomUUID().toString()));      
    }
    base.setOperations(ops);
    CompoundRequest copy = new CompoundRequest();
    copy(base, copy);
    deepEquals(base, copy);
  }
  protected static final int TEST_OPERATION_ID = 0;
  protected static class OperationTest extends OperationRequest {
    protected String payload;
    public OperationTest() {
      this(null);
    }
    public OperationTest(String payload) {
      this.payload = payload;
    }
    @Override
    public void read(RPCBuffer buffer) {
      payload = buffer.readString();
    }
    @Override
    public void write(RPCBuffer buffer) {
      buffer.writeString(payload);
    }
    public int getID() {
      return TEST_OPERATION_ID;
    }
    public String getPayload() {
      return payload;
    }
    public String toString() {
      return getPayload();
    }
  }

}
