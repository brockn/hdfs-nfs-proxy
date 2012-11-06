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

import com.cloudera.hadoop.hdfs.nfs.security.Credentials;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsGSS;
import com.cloudera.hadoop.hdfs.nfs.security.Verifier;

/**
 * Represents an RPC Request as defined by the RPC RFC.
 */
public class RPCRequest extends RPCPacket implements Cloneable {

  private int mCredentialsFlavor;
  private Credentials mCredentials;
  private Verifier mVerifier;
  private int mRpcVersion, mProgram, mProgramVersion, mProcedure;

  public RPCRequest() {

  }

  public RPCRequest(int xid, int rpcVersion) {
    setXid(xid);
    setMessageType(RPC_MESSAGE_TYPE_CALL);
    this.mRpcVersion = rpcVersion;
  }

  public Credentials getCredentials() {
    return mCredentials;
  }

  public int getProcedure() {
    return mProcedure;
  }
  public int getProgram() {
    return mProgram;
  }
  public int getProgramVersion() {
    return mProgramVersion;
  }
  public int getRpcVersion() {
    return mRpcVersion;
  }
  public Verifier getVerifier() {
    return mVerifier;
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
    mVerifier = null;
    if(!(mCredentials instanceof CredentialsGSS && 
        ((CredentialsGSS) mCredentials).getProcedure() == RPCSEC_GSS_DESTROY)) {
      int verifierFlavor = buffer.readUint32();
      mVerifier = Verifier.readVerifier(verifierFlavor, buffer);
    }
  }
  public void setCredentials(Credentials credentials) {
    mCredentials = credentials;
    mCredentialsFlavor = mCredentials.getFlavor();
  }
  public void setProcedure(int procedure) {
    this.mProcedure = procedure;
  }
  public void setProgram(int program) {
    this.mProgram = program;
  }
  public void setProgramVersion(int programVersion) {
    this.mProgramVersion = programVersion;
  }
  public void setRpcVersion(int rpcVersion) {
    this.mRpcVersion = rpcVersion;
  }
  public void setVerifier(Verifier verifier) {
    mVerifier = verifier;
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
      buffer.writeUint32(mVerifier.getFlavor());
      mVerifier.write(buffer);
    }
  }
  
  public byte[] getVerificationBuffer() {
    RPCBuffer buffer = new RPCBuffer();
    super.write(buffer);
    buffer.writeUint32(mRpcVersion);
    buffer.writeUint32(mProgram);
    buffer.writeUint32(mProgramVersion);
    buffer.writeUint32(mProcedure);
    buffer.writeUint32(mCredentialsFlavor);
    mCredentials.write(buffer);
    buffer.flip();
    return buffer.readBytes(buffer.limit());
  }
}