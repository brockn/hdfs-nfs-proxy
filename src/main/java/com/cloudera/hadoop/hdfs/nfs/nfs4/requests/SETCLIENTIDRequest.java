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
package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Callback;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class SETCLIENTIDRequest extends OperationRequest {

  protected ClientID mClientID;
  protected Callback mCallback;
  protected int mCallbackIdent;

  public Callback getCallback() {
    return mCallback;
  }

  public int getCallbackIdent() {
    return mCallbackIdent;
  }

  public ClientID getClientID() {
    return mClientID;
  }

  @Override
  public int getID() {
    return NFS4_OP_SETCLIENTID;
  }

  @Override
  public void read(RPCBuffer buffer) {
    mClientID = new ClientID();
    mClientID.read(buffer);
    mCallback = new Callback();
    mCallback.read(buffer);
    mCallbackIdent = buffer.readUint32();
  }

  public void setCallback(Callback callback) {
    this.mCallback = callback;
  }

  public void setCallbackIdent(int callbackIdent) {
    this.mCallbackIdent = callbackIdent;
  }

  public void setClientID(ClientID clientID) {
    this.mClientID = clientID;
  }

  @Override
  public void write(RPCBuffer buffer) {
    mClientID.write(buffer);
    mCallback.write(buffer);
    buffer.writeUint32(mCallbackIdent);
  }

}
