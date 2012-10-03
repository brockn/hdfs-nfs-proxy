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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_READ;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class READResponse extends OperationResponse implements Status {

  protected int mStatus;
  protected boolean mEOF;
  protected byte[] mData;
  protected int mStart;
  protected int mLength;

  @Override
  public void read(RPCBuffer buffer) {
    reset();
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      mEOF = buffer.readBoolean();
      mData = buffer.readBytes();
      mStart = 0;
      mLength = mData.length;
    }
  }

  protected void reset() {
    mData = null;
  }
  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mStatus == NFS4_OK) {
      buffer.writeBoolean(mEOF);
      buffer.writeUint32(mLength);
      buffer.writeBytes(mData, mStart, mLength);
    }
  }

  public boolean isEOF() {
    return mEOF;
  }

  public void setEOF(boolean EOF) {
    this.mEOF = EOF;
  }

  public int getStart() {
    return mStart;
  }
  public int getLength() {
    return mLength;
  }
  public byte[] getData() {
    return mData;
  }

  public void setData(byte[] data, int start, int length) {
    this.mData = data;
    this.mStart = start;
    this.mLength = length;
  }

  @Override
  public int getStatus() {
    return mStatus;
  }
  @Override
  public void setStatus(int status) {
    this.mStatus = status;
  }
  @Override
  public int getID() {
    return NFS4_OP_READ;
  }
}
