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
package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.apache.log4j.Logger;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.MessageProp;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.NFSUtils;
import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;

public class GSSSecurityHandler extends SecurityHandler {
  protected static final Logger LOGGER = Logger.getLogger(GSSSecurityHandler.class);
  protected GSSManager mManager = GSSManager.getInstance();
  protected GSSContext mContext;
  protected byte[] mToken;
  protected int mSequenceNumber;
  protected byte[] mContextID = Bytes.toBytes(NFSUtils.nextRandomInt());

  public GSSSecurityHandler() throws GSSException {
    mContext = mManager.createContext((GSSCredential) null);
  }

  @Override
  public boolean hasAcceptableSecurity(RPCRequest request) {
    return request.getCredentials() != null && request.getCredentials() instanceof CredentialsGSS;
  }

  @Override
  public Pair<? extends Verifier, RPCBuffer> initializeContext(RPCRequest request, RPCBuffer buffer) throws NFS4Exception {
    try {
      if(!mContext.isEstablished()) {
        int length = buffer.readUint32();
        mToken = buffer.readBytes(length);
        System.out.println("Reading token " + length + ": " + Bytes.asHex(mToken));
        mToken = mContext.acceptSecContext(mToken, 0, mToken.length);
        if(mToken == null) {
          mToken = new byte[0];
        }
        System.out.println("Writing token " + mToken.length + ": " + Bytes.asHex(mToken));
        System.out.println("Established " + mContext.isEstablished());
      }

      System.out.println(mContext.getSrcName());

      CredentialsGSS creds = (CredentialsGSS)request.getCredentials();
      mSequenceNumber = creds.getSequenceNum();

      RPCBuffer payload = new RPCBuffer();
      payload.writeUint32(mContextID.length);
      payload.writeBytes(mContextID);
      payload.writeUint32(0); // major
      payload.writeUint32(0); // minor
      payload.writeUint32(128); // sequence window
      payload.writeUint32(mToken.length);
      payload.writeBytes(mToken);
      byte[] sequenceNumber = Bytes.toBytes(128);
      MessageProp msgProp = new MessageProp(false);
      VerifierGSS verifier = new VerifierGSS();
      verifier.set(mContext.getMIC(sequenceNumber, 0, sequenceNumber.length, msgProp));
      return Pair.of(verifier, payload);
    } catch(GSSException ex) {
      LOGGER.warn("Error in initializeContext", ex);
      throw new NFS4Exception(NFS4ERR_PERM, ex);
    }
  }

  @Override
  public Verifier getVerifer(RPCRequest request) throws NFS4Exception {
    CredentialsGSS creds = (CredentialsGSS)request.getCredentials();
    int equenceNumber = creds.getSequenceNum();
    byte[] sequenceNumber = Bytes.toBytes(equenceNumber);
    MessageProp msgProp = new MessageProp(false);
    VerifierGSS verifier = new VerifierGSS();
    try {
      verifier.set(mContext.getMIC(sequenceNumber, 0, sequenceNumber.length, msgProp));
    } catch(GSSException ex) {
      LOGGER.warn("Error in getMIC", ex);
      throw new NFS4Exception(NFS4ERR_PERM, ex);
    }
    return verifier;
  }

  @Override
  public boolean isWrapRequired() {
    return true;
  }

  @Override
  public boolean isUnwrapRequired() {
    return true;
  }

  @Override
  public byte[] unwrap(byte[] data) throws NFS4Exception {
    try {
      return mContext.unwrap(data, 0, data.length, new MessageProp(true));
    } catch(GSSException ex) {
      LOGGER.warn("Error in getMIC", ex);
      throw new NFS4Exception(NFS4ERR_PERM, ex);
    }
  }

  @Override
  public byte[] wrap(MessageBase response) throws NFS4Exception {
    RPCBuffer buffer = new RPCBuffer();
    response.write(buffer);
    buffer.flip();
    byte[] data = buffer.readBytes(buffer.length());
    try {
      return mContext.wrap(data, 0, data.length, new MessageProp(true));
    } catch(GSSException ex) {
      LOGGER.warn("Error in getMIC", ex);
      throw new NFS4Exception(NFS4ERR_PERM, ex);
    }
  }
}
