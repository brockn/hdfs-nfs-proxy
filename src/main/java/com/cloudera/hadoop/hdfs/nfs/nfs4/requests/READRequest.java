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
package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_READ;

import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class READRequest extends OperationRequest {
  protected StateID mStateID;
  protected long mOffset;
  protected int mCount;

  @Override
  public void read(RPCBuffer buffer) {
    mStateID = new StateID();
    mStateID.read(buffer);
    mOffset = buffer.readUint64();
    mCount = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    mStateID.write(buffer);
    buffer.writeUint64(mOffset);
    buffer.writeUint32(mCount);
  }

  @Override
  public int getID() {
    return NFS4_OP_READ;
  }

  public StateID getStateID() {
    return mStateID;
  }

  public void setStateID(StateID stateID) {
    this.mStateID = stateID;
  }

  public long getOffset() {
    return mOffset;
  }

  public void setOffset(long offset) {
    this.mOffset = offset;
  }

  public int getCount() {
    return mCount;
  }

  public void setCount(int count) {
    this.mCount = count;
  }

}
