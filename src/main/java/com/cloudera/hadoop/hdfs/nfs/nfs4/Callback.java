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

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class Callback implements MessageBase {
  protected int mCallbackProgram;
  protected String mNetID;
  protected String mAddr;

  @Override
  public void read(RPCBuffer buffer) {
    mCallbackProgram = buffer.readUint32();
    mNetID = buffer.readString();
    mAddr = buffer.readString();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mCallbackProgram);
    buffer.writeString(mNetID);
    buffer.writeString(mAddr);
  }



  public int getCallbackProgram() {
    return mCallbackProgram;
  }

  public void setCallbackProgram(int mCallbackProgram) {
    this.mCallbackProgram = mCallbackProgram;
  }

  public String getNetID() {
    return mNetID;
  }

  public void setNetID(String mNetID) {
    this.mNetID = mNetID;
  }

  public String getAddr() {
    return mAddr;
  }

  public void setAddr(String mAddr) {
    this.mAddr = mAddr;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mAddr == null) ? 0 : mAddr.hashCode());
    result = prime * result + mCallbackProgram;
    result = prime * result + ((mNetID == null) ? 0 : mNetID.hashCode());
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
    Callback other = (Callback) obj;
    if (mAddr == null) {
      if (other.mAddr != null)
        return false;
    } else if (!mAddr.equals(other.mAddr))
      return false;
    if (mCallbackProgram != other.mCallbackProgram)
      return false;
    if (mNetID == null) {
      if (other.mNetID != null)
        return false;
    } else if (!mNetID.equals(other.mNetID))
      return false;
    return true;
  }
}
