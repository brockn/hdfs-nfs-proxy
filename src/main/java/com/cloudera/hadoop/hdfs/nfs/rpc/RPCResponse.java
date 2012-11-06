/**
 * Copyright 2012 The Apache Software Foundation
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
  protected Verifier mVerifier;

  public RPCResponse() {

  }
  public RPCResponse(int xid, int rpcVersion) {
    setXid(xid);
    setMessageType(RPC_MESSAGE_TYPE_REPLY);
    this.mReplyState = RPC_REPLY_STATE_ACCEPT;
    this.mAcceptState = RPC_ACCEPT_STATE_ACCEPT;
  }

  public int getAcceptState() {
    return mAcceptState;
  }

  public int getAuthState() {
    return mAuthState;
  }
  public int getReplyState() {
    return mReplyState;
  }
  public Verifier getVerifier() {
    return mVerifier;
  }
  @Override
  public void read(RPCBuffer buffer) {
    super.read(buffer);
    this.mReplyState = buffer.readUint32();
    /*
     * It looks like if reply state is not
     * accept, the next value acceptState
     */
    if(mReplyState == RPC_REPLY_STATE_ACCEPT) {
      int verifierFlavor = buffer.readUint32();
      mVerifier = Verifier.readVerifier(verifierFlavor, buffer);
      mAcceptState = buffer.readUint32();
    } else if(mReplyState == RPC_REPLY_STATE_DENIED) {
      mAcceptState = buffer.readUint32();
      if(mAcceptState == RPC_REJECT_AUTH_ERROR) {
        mAuthState = buffer.readUint32();
      }
    }
  }
  public void setAcceptState(int acceptState) {
    this.mAcceptState = acceptState;
  }
  public void setAuthState(int authState) {
    this.mAuthState = authState;
  }
  public void setReplyState(int replyState) {
    this.mReplyState = replyState;
  }
  public void setVerifier(Verifier verifier) {
    mVerifier = verifier;
  }
  @Override
  public String toString() {
    return "RPCResponse [getVerifier()=" + getVerifier() + ", getAuthState()="
        + getAuthState() + getAcceptState() + ", getReplyState()="
        + getReplyState() + ", getXidAsHexString()=" + getXidAsHexString()
        + ", getXid()=" + getXid() + ", getMessageType()=" + getMessageType()
        + "]";
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
      buffer.writeUint32(mVerifier.getFlavor());
      mVerifier.write(buffer);
      buffer.writeUint32(mAcceptState);
    } else if(mReplyState == RPC_REPLY_STATE_DENIED) {
      buffer.writeUint32(mAcceptState);
      if(mAcceptState == RPC_REJECT_AUTH_ERROR) {
        buffer.writeUint32(mAuthState);
      }

    }
  }
}
