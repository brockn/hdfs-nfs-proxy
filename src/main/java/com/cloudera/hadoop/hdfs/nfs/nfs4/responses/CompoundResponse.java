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
package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;


import java.util.List;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OperationFactory;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class CompoundResponse implements MessageBase, Status {

  protected ImmutableList<OperationResponse> mOperations = ImmutableList.<OperationResponse>builder().build();
  protected byte[] mTags = new byte[0];
  protected int mStatus;
  
  @Override
  public void read(RPCBuffer buffer) {
    mStatus = buffer.readUint32();
    mTags = buffer.readBytes();
    int count = buffer.readUint32();
    List<OperationResponse> ops = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      int id = buffer.readUint32();
      if(OperationFactory.isSupported(id)) {
        ops.add(OperationFactory.parseResponse(id, buffer));        
      }
    }
    mOperations = ImmutableList.<OperationResponse>builder().addAll(ops).build();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    buffer.writeUint32(mTags.length);
    buffer.writeBytes(mTags);
    buffer.writeUint32(mOperations.size());
    for(OperationResponse operation : mOperations) {
      buffer.writeUint32(operation.getID());
      operation.write(buffer);
    }
  }

  public void setOperations(List<OperationResponse> operations) {
    mOperations = ImmutableList.copyOf(operations);
  }
  public ImmutableList<OperationResponse> getOperations() {
    return mOperations;
  }
  @Override
  public int getStatus() {
    return mStatus;
  }
  @Override
  public void setStatus(int status) {
    mStatus = status;
  }
}
