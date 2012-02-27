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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class SETCLIENTIDCONFIRMRequest extends OperationRequest {
  protected long mClientID;
  protected OpaqueData8 mVerifer;
  @Override
  public void read(RPCBuffer buffer) {
    mClientID = buffer.readUint64();
    mVerifer = new OpaqueData8();
    mVerifer.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mClientID);
    mVerifer.write(buffer);
  }

  @Override
  public int getID() {
    return NFS4_OP_SETCLIENTID_CONFIRM;
  }

  public long getClientID() {
    return mClientID;
  }

  public void setClientID(long clientID) {
    this.mClientID = clientID;
  }

  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }

}
