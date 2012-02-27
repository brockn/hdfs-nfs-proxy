/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.security.Verifier;

/**
 * Represents a RPC Response as defined by the RPC RFC.
 */
public class RPCResponse extends RPCPacket {
  protected static final Logger LOGGER = Logger.getLogger(RPCResponse.class);

  protected int mReplyState, mAcceptState, mAuthState;
  protected int mVerifierFlavor;
  protected Verifier mVerifier;

  public RPCResponse() {

  }
  public RPCResponse(int xid, int rpcVersion) {
    this.mXid = xid;
    this.mRpcVersion = rpcVersion;

    this.mMessageType = RPC_MESSAGE_TYPE_REPLY;
    this.mReplyState = RPC_REPLY_STATE_ACCEPT;
    this.mAcceptState = RPC_ACCEPT_STATE_ACCEPT;
  }

  @Override
  public void write(RPCBuffer buffer) {
    super.write(buffer);

    buffer.writeUint32(mReplyState);
    /*
     * It looks like if reply state is not
     * accept, the next value acceptState
     */
    if(mReplyState == RPC_REPLY_STATE_ACCEPT) {
      buffer.writeUint32(mVerifierFlavor);
      mVerifier.write(buffer);
      buffer.writeUint32(mAcceptState);
    } else if(mReplyState == RPC_REPLY_STATE_DENIED) {
      buffer.writeUint32(mAcceptState);
      if(mAcceptState == RPC_REJECT_AUTH_ERROR) {
        buffer.writeUint32(mAuthState);
      }

    }
  }

  @Override
  public void read(RPCBuffer buffer) {
    super.read(buffer);
    this.mReplyState = buffer.readInt();
    /*
     * It looks like if reply state is not
     * accept, the next value acceptState
     */
    if(mReplyState == RPC_REPLY_STATE_ACCEPT) {
      mVerifierFlavor = buffer.readUint32();
      mVerifier = Verifier.readVerifier(mVerifierFlavor, buffer);
      this.mAcceptState = buffer.readUint32();
    } else if(mReplyState == RPC_REPLY_STATE_DENIED) {
      this.mAcceptState = buffer.readUint32();
      if(mAcceptState == RPC_REJECT_AUTH_ERROR) {
        mAuthState = buffer.readUint32();
      }
    }
  }
  public Verifier getVerifier() {
    return mVerifier;
  }
  public void setVerifier(Verifier verifier) {
    mVerifier = verifier;
    mVerifierFlavor = mVerifier.getFlavor();
  }
  public int getAuthState() {
    return mAuthState;
  }
  public void setAuthState(int authState) {
    this.mAuthState = authState;
  }
  @Override
  public String toString() {
    return "RPCResponse [replyState=" + mReplyState + ", acceptState="
        + mAcceptState + "]";
  }
  public int getVerifierFlavor() {
    return mVerifierFlavor;
  }
  public void setVerifierFlavor(int verifierFlavor) {
    this.mVerifierFlavor = verifierFlavor;
  }
  public int getAcceptState() {
    return mAcceptState;
  }
  public void setAcceptState(int acceptState) {
    this.mAcceptState = acceptState;
  }
  public int getReplyState() {
    return mReplyState;
  }
  public void setReplyState(int replyState) {
    this.mReplyState = replyState;
  }
}
