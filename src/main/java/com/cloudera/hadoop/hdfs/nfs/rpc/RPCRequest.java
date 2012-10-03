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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_DESTROY;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_MESSAGE_TYPE_CALL;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.security.Credentials;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsGSS;
import com.cloudera.hadoop.hdfs.nfs.security.Verifier;

/**
 * Represents an RPC Request as defined by the RPC RFC.
 */
public class RPCRequest extends RPCPacket {

  protected static final Logger LOGGER = Logger.getLogger(RPCRequest.class);
  protected int mCredentialsFlavor;
  protected Credentials mCredentials;
  protected int mVerifierFlavor;
  protected Verifier mVerifier;

  public RPCRequest(int xid, int rpcVersion) {
    this.mXid = xid;

    this.mMessageType = RPC_MESSAGE_TYPE_CALL;
    this.mRpcVersion = rpcVersion;


  }

  public RPCRequest() {
  }

  @Override
  public void write(RPCBuffer buffer ) {
    super.write(buffer);
    buffer.writeUint32(mRpcVersion);
    buffer.writeUint32(mProgram);
    buffer.writeUint32(mProgramVersion);
    buffer.writeUint32(mProcedure);
    buffer.writeUint32(mCredentialsFlavor);
    mCredentials.write(buffer);
    // verifier can be null if we are calculating
    // the checksum before sending a packet
    if(mVerifier != null) {
      buffer.writeUint32(mVerifierFlavor);
      mVerifier.write(buffer);
    }
  }


  @Override
  public void read(RPCBuffer buffer) {
    super.read(buffer);
    mRpcVersion = buffer.readUint32();
    mProgram = buffer.readUint32();
    mProgramVersion = buffer.readUint32();
    mProcedure = buffer.readUint32();
    mCredentialsFlavor = buffer.readUint32();
    mCredentials = Credentials.readCredentials(mCredentialsFlavor, buffer);
    if(!(mCredentials instanceof CredentialsGSS && ((CredentialsGSS) mCredentials).getProcedure() == RPCSEC_GSS_DESTROY)) {
      mVerifierFlavor = buffer.readUint32();
      mVerifier = Verifier.readVerifier(mVerifierFlavor, buffer);
    }
  }
  public Credentials getCredentials() {
    return mCredentials;
  }
  public void setCredentials(Credentials credentials) {
    mCredentials = credentials;
    mCredentialsFlavor = mCredentials.getFlavor();
  }
  public Verifier getVerifier() {
    return mVerifier;
  }
  public void setVerifier(Verifier verifier) {
    mVerifier = verifier;
    mVerifierFlavor = mVerifier.getFlavor();
  }
}