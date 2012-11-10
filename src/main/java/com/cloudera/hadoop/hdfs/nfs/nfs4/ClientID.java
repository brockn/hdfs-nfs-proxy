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

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class ClientID implements MessageBase {
  protected OpaqueData8 mVerifer;
  protected OpaqueData mID;
  public OpaqueData getOpaqueID() {
    return mID;
  }

  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  @Override
  public void read(RPCBuffer buffer) {
    mVerifer = new OpaqueData8();
    mVerifer.read(buffer);
    mID = new OpaqueData(buffer.readUint32());
    mID.read(buffer);
  }

  public void setOpaqueID(OpaqueData id) {
    this.mID = id;
  }

  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }

  @Override
  public void write(RPCBuffer buffer) {
    mVerifer.write(buffer);
    buffer.writeUint32(mID.getSize());
    mID.write(buffer);
  }
}
