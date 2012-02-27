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

public class ClientID implements MessageBase {
  protected OpaqueData8 mVerifer;
  protected OpaqueData mID;
  @Override
  public void read(RPCBuffer buffer) {
    mVerifer = new OpaqueData8();
    mVerifer.read(buffer);
    mID = new OpaqueData(buffer.readUint32());
    mID.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    mVerifer.write(buffer);
    buffer.writeUint32(mID.getSize());
    mID.write(buffer);
  }

  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }

  public OpaqueData getOpaqueID() {
    return mID;
  }

  public void setOpaqueID(OpaqueData id) {
    this.mID = id;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mID == null) ? 0 : mID.hashCode());
    result = prime * result + ((mVerifer == null) ? 0 : mVerifer.hashCode());
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
    ClientID other = (ClientID) obj;
    if (mID == null) {
      if (other.mID != null)
        return false;
    } else if (!mID.equals(other.mID))
      return false;
    if (mVerifer == null) {
      if (other.mVerifer != null)
        return false;
    } else if (!mVerifer.equals(other.mVerifer))
      return false;
    return true;
  }
}
