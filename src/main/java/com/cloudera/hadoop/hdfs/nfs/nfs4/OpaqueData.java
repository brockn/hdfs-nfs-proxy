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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.util.Arrays;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

/**
 * Represents a specific number of opaque bytes
 */
public class OpaqueData implements MessageBase {

  protected byte[] mData;
  protected int mSize;
  public OpaqueData(byte[] data) {
    this(data.length);
    setData(data);
  }
  public OpaqueData(int size) {
    mSize = size;
    if(mSize > NFS4_OPAQUE_LIMIT) {
      throw new RuntimeException("Size too large " + mSize);
    }
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    OpaqueData other = (OpaqueData) obj;
    if (!Arrays.equals(mData, other.mData))
      return false;
    if (mSize != other.mSize)
      return false;
    return true;
  }
  public byte[] getData() {
    return mData;
  }
  public int getSize() {
    return mSize;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(mData);
    result = prime * result + mSize;
    return result;
  }

  @Override
  public void read(RPCBuffer buffer) {
    mData = buffer.readBytes(mSize);
  }
  public void setData(byte[] data) {
    mData = Arrays.copyOf(data, mSize);
  }
  public void setSize(int size) {
    mSize = size;
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeBytes(mData, 0, mSize);
  }
}
