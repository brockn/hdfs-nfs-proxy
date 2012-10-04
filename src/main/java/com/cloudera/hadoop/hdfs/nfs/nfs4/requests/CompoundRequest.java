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
package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import java.util.List;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OperationFactory;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class CompoundRequest implements MessageBase, RequiresCredentials {

  protected static final Logger LOGGER = Logger.getLogger(CompoundRequest.class);
  protected int mMinorVersion;
  protected byte[] mTags = new byte[0];
  protected AuthenticatedCredentials mCredentials;
  protected ImmutableList<OperationRequest> mOperations = ImmutableList.<OperationRequest>builder().build();

  public CompoundRequest() {
  }

  @Override
  public void read(RPCBuffer buffer) {
    mTags = buffer.readBytes();
    mMinorVersion = buffer.readUint32();
    int count = buffer.readUint32();
    List<OperationRequest> ops = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      int id = buffer.readUint32();
      if (OperationFactory.isSupported(id)) {
        ops.add(OperationFactory.parseRequest(id, buffer));
      } else {
        LOGGER.warn("Dropping request with id " + id + ": " + ops);
        throw new UnsupportedOperationException("NFS ID " + id);
      }
    }
    mOperations = ImmutableList.<OperationRequest>builder().addAll(ops).build();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mTags.length);
    buffer.writeBytes(mTags);
    buffer.writeUint32(mMinorVersion);
    buffer.writeUint32(mOperations.size());
    for (OperationRequest operation : mOperations) {
      buffer.writeUint32(operation.getID());
      operation.write(buffer);
    }
  }

  public int getMinorVersion() {
    return mMinorVersion;
  }

  public void setMinorVersion(int mMinorVersion) {
    this.mMinorVersion = mMinorVersion;
  }

  public void setOperations(List<OperationRequest> operations) {
    mOperations = ImmutableList.<OperationRequest>copyOf(operations);
  }

  public ImmutableList<OperationRequest> getOperations() {
    return mOperations;
  }

  @Override
  public AuthenticatedCredentials getCredentials() {
    return mCredentials;
  }

  @Override
  public String toString() {
    return this.getClass().getName() + " = " + mOperations.toString();
  }
  @Override
  public void setCredentials(AuthenticatedCredentials mCredentials) {
    this.mCredentials = mCredentials;
  }

  public static CompoundRequest from(RPCBuffer buffer) {
    CompoundRequest request = new CompoundRequest();
    request.read(buffer);
    return request;
  }
}
