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

import java.util.concurrent.atomic.AtomicLong;

public class Client {

  protected static final AtomicLong CLIENTID = new AtomicLong(0L);


  protected ClientID mClientID;
  protected Callback mCallback;
  protected int mCallbackIdent;
  protected long mShorthandID;
  protected OpaqueData8 mVerifer;
  protected String mClientHost;
  protected boolean mConfirmed;
  protected long mRenew = System.currentTimeMillis();

  public boolean isConfirmed() {
    return mConfirmed;
  }

  public void setConfirmed(boolean confirmed) {
    this.mConfirmed = confirmed;
  }

  protected Client(ClientID clientID) {
    this.mClientID = clientID;
    mShorthandID = CLIENTID.addAndGet(10L);
  }

  public ClientID getClientID() {
    return mClientID;
  }

  public void setClientID(ClientID clientID) {
    this.mClientID = clientID;
  }

  public Callback getCallback() {
    return mCallback;
  }

  public void setCallback(Callback callback) {
    this.mCallback = callback;
  }

  public int getCallbackIdent() {
    return mCallbackIdent;
  }

  public void setCallbackIdent(int callbackIdent) {
    this.mCallbackIdent = callbackIdent;
  }

  public long getShorthandID() {
    return mShorthandID;
  }

  public void setShorthandID(long shorthandID) {
    this.mShorthandID = shorthandID;
  }
  public void setClientHost(String host) {
    this.mClientHost = host;
  }
  public String getClientHost() {
    return mClientHost;
  }
  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }
  public void setRenew(long ts) {
    mRenew = ts;
  }
}
