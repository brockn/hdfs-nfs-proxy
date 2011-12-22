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


import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class StateID implements MessageBase {
  protected int mSeqID;
  protected OpaqueData12 mData;
  protected static final int LOCAL_ID = (new Random()).nextInt();
  protected static final AtomicLong STATEIDs = new AtomicLong(0);
  
  @Override
  public void read(RPCBuffer buffer) {
    mSeqID = buffer.readUint32();
    mData = new OpaqueData12();
    mData.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mSeqID);
    mData.write(buffer);
  }
  
  

  public int getSeqID() {
    return mSeqID;
  }

  public void setSeqID(int seqID) {
    this.mSeqID = seqID;
  }

  public OpaqueData12 getData() {
    return mData;
  }

  public void setData(OpaqueData12 data) {
    this.mData = data;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mData == null) ? 0 : mData.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    StateID other = (StateID) obj;
    if (mData == null) {
      if (other.mData != null)
        return false;
    } else if (!mData.equals(other.mData))
      return false;
    return true;
  }
  public synchronized static StateID newStateID(int seqID) {
    long counter = STATEIDs.addAndGet(10L);
    StateID stateID = new StateID();
    OpaqueData12 data = new OpaqueData12();
    data.setData(Bytes.add(Bytes.toBytes(LOCAL_ID), Bytes.toBytes(counter)));
    stateID.setData(data);
    stateID.setSeqID(seqID);
    return stateID;
  }
}
