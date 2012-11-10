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
package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Callback;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;

public class Client {
  protected static final AtomicLong CLIENTID = new AtomicLong(0L);

  private ClientID mClientID;
  private Callback mCallback;
  private int mCallbackIdent;
  private long mShorthandID;
  private OpaqueData8 mVerifer;
  private String mClientHost;
  private boolean mConfirmed;

  protected Client(ClientID clientID) {
    this.mClientID = clientID;
    mShorthandID = CLIENTID.addAndGet(10L);
  }

  public Callback getCallback() {
    return mCallback;
  }

  public int getCallbackIdent() {
    return mCallbackIdent;
  }

  public String getClientHost() {
    return mClientHost;
  }

  public ClientID getClientID() {
    return mClientID;
  }

  public long getShorthandID() {
    return mShorthandID;
  }

  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  public boolean isConfirmed() {
    return mConfirmed;
  }

  public void setCallback(Callback callback) {
    this.mCallback = callback;
  }

  public void setCallbackIdent(int callbackIdent) {
    this.mCallbackIdent = callbackIdent;
  }

  public void setClientHost(String host) {
    this.mClientHost = host;
  }
  public void setClientID(ClientID clientID) {
    this.mClientID = clientID;
  }
  public void setConfirmed(boolean confirmed) {
    this.mConfirmed = confirmed;
  }
  public void setRenew(long ts) {
    // not used for now
  }

  public void setShorthandID(long shorthandID) {
    this.mShorthandID = shorthandID;
  }
  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }
}
