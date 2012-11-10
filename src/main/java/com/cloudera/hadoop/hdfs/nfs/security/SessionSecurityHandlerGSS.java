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

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.MessageProp;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCAcceptedException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCAuthException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class SessionSecurityHandlerGSS extends SessionSecurityHandler<VerifierGSS> {
  private static final Logger LOGGER = Logger.getLogger(SessionSecurityHandlerGSS.class);
  @SuppressWarnings("unused")
  private final CredentialsGSS mCredentialsGSS;
  private final GSSManager mManager;
  private final GSSContext mContext;
  private final byte[] mContextID;
  private final String mSuperUser;
  private final SortedSet<Integer> mSequenceNumbers;
  private byte[] mToken;
  
  public SessionSecurityHandlerGSS(CredentialsGSS crentialsGSS, 
      GSSManager manager, int contextID, String superUser) {
    mCredentialsGSS = crentialsGSS;
    mManager = manager;
    try {
      mContext = mManager.createContext((GSSCredential) null);      
    } catch (GSSException e) {
      throw Throwables.propagate(e);
    }
    mContextID = Bytes.toBytes(contextID);
    mSuperUser = superUser;
    mSequenceNumbers = Collections.synchronizedSortedSet(new TreeSet<Integer>());
  }

  public byte[] getContextID() {
    return mContextID;
  }
  
  @Override
  public synchronized String getUser() 
      throws NFS4Exception {
    try {
      if(mContext.isEstablished()) {
        String user = mContext.getSrcName().toString();
        // TODO hack, hack hack, fix this
        if(user.contains("@")) {
          user = user.substring(0, user.indexOf("@"));
        }
        if(user.contains("/")) {
          user = user.substring(0, user.indexOf("/"));
        }
        if(user.equalsIgnoreCase("nfs")) {
          return mSuperUser;
        }
        return user;
      }
      return ANONYMOUS_USERNAME; 
    } catch (Exception e) {
      throw new NFS4Exception(NFS4ERR_SERVERFAULT, e);
    }
  }
  
  @Override
  public synchronized VerifierGSS getVerifer(RPCRequest request) throws RPCException {
    CredentialsGSS creds = (CredentialsGSS)request.getCredentials();
    byte[] sequenceNumber = Bytes.toBytes(creds.getSequenceNum());
    MessageProp msgProp = new MessageProp(false);
    VerifierGSS verifier = new VerifierGSS();
    try {
      verifier.set(mContext.getMIC(sequenceNumber, 0, sequenceNumber.length, msgProp));
    } catch(GSSException ex) {
      throw new RPCAuthException(RPC_AUTH_STATUS_GSS_CTXPROBLEM, ex);
    }
    return verifier;
  }
  public synchronized Pair<? extends Verifier, InitializeResponse> initializeContext(RPCRequest request, RPCBuffer buffer) 
    throws RPCException {
    CredentialsGSS creds = (CredentialsGSS)request.getCredentials();
    Preconditions.checkState(creds.getProcedure() == RPCSEC_GSS_INIT || 
        creds.getProcedure() == RPCSEC_GSS_CONTINUE_INIT, "Proc is " + creds.getFlavor());
    Preconditions.checkState(request.getVerifier() instanceof VerifierNone, 
        String.valueOf(request.getVerifier()));
    if(!mContext.isEstablished()) {
      int length = buffer.readUint32();
      mToken = buffer.readBytes(length);
      try {
        mToken = mContext.acceptSecContext(mToken, 0, mToken.length);
      } catch (GSSException e) {
        LOGGER.warn("Error on accept: major = " + e.getMajor() + 
            ", minor = " + e.getMinor() + ", major = " + e.getMajorString() + 
            ", minor = " + e.getMinorString(), e);
        InitializeResponse initResponse = new InitializeResponse();
        initResponse.setContextID(null);
        initResponse.setMajorErrorCode(e.getMajor());
        initResponse.setMinorErrorCode(e.getMinor());
        initResponse.setSequenceWindow(RPCSEC_GSS_SEQUENCE_WINDOW);
        initResponse.setToken(null);
        return Pair.of(new VerifierNone(), initResponse);
      }
      if(mToken == null) {
        mToken = new byte[0];
      }
      String msg = "Initializing context: Reading token " + length + 
          "Writing token " + mToken.length + ", isEstablished = " + 
          mContext.isEstablished();
      LOGGER.info(msg);
    }
    if(mContext.isEstablished()) {
      try {
        String msg = "Initialized context: source = " + mContext.getSrcName() +
            ", target " + mContext.getTargName();
        LOGGER.info(msg);        
      } catch (GSSException e) {
        LOGGER.warn("Error trying to obtain source and target names" + e);
      }
    }
    // contextID (handle) and token are null when major is not COMPLETE or CONTINUE_NEEDED
    InitializeResponse initResponse = new InitializeResponse();
    initResponse.setContextID(mContextID);
    initResponse.setMajorErrorCode(RPCSEC_GSS_COMPLETE);
    initResponse.setMinorErrorCode(0);
    initResponse.setSequenceWindow(RPCSEC_GSS_SEQUENCE_WINDOW);
    initResponse.setToken(mToken);
    byte[] sequenceNumber = Bytes.toBytes(RPCSEC_GSS_SEQUENCE_WINDOW);
    MessageProp msgProp = new MessageProp(false);
    VerifierGSS verifier = new VerifierGSS();
    try {
      verifier.set(mContext.getMIC(sequenceNumber, 0, sequenceNumber.length, msgProp));      
    } catch (GSSException ex) {
      throw new RPCAuthException(RPC_AUTH_STATUS_GSS_CTXPROBLEM, ex);
    }
    return Pair.of(verifier, initResponse);
  }
  @Override
  public boolean isUnwrapRequired() {
    return true;
  }
  @Override
  public boolean isWrapRequired() {
    return true;
  }
  @Override
  public synchronized boolean shouldSilentlyDrop(RPCRequest request) {
    CredentialsGSS creds = (CredentialsGSS)request.getCredentials();
    Preconditions.checkState(creds.getProcedure() == RPCSEC_GSS_DATA);
    Preconditions.checkState(creds.getService()== RPCSEC_GSS_SERVICE_PRIVACY, 
        String.valueOf(creds.getService()));
    /*
     * Request should be dropped if we have seen the request or if it
     * falls below the current sequencer number window.
     */
    if(creds.getSequenceNum() > RPCSEC_GSS_MAX_SEQUENCE_NUMBER) {
      LOGGER.info("Dropping " + request.getXidAsHexString() + " large sequence number");
      return true;
    } else if(mSequenceNumbers.contains(creds.getSequenceNum())) {
      LOGGER.info("Dropping " + request.getXidAsHexString() + " due to duplicate sequence numbers");
      return true;
    } else if(!mSequenceNumbers.isEmpty() && 
        creds.getSequenceNum() < mSequenceNumbers.first()) {
      LOGGER.info("Dropping " + request.getXidAsHexString() + " low sequence number");
      return true;
    }
    return false;
  }
  // TODO need to understand mechanics of getSrcName
  // meaning when does it change to the actual user
  @Override
  public synchronized RPCBuffer unwrap(RPCRequest request, byte[] data) throws RPCException {
    CredentialsGSS creds = (CredentialsGSS)request.getCredentials();
    Preconditions.checkState(creds.getProcedure() == RPCSEC_GSS_DATA);
    Preconditions.checkState(creds.getService()== RPCSEC_GSS_SERVICE_PRIVACY, 
        String.valueOf(creds.getService()));
    if(LOGGER.isDebugEnabled()) {
      try {
        LOGGER.debug("Unwrapped request for " + mContext.getSrcName());
      } catch (GSSException e) {
        LOGGER.warn("Error getting source name", e);
      }
    }
    MessageProp msgProp = new MessageProp(true);
    byte[] inToken = ((VerifierGSS)request.getVerifier()).get();      
    byte[] inMsg = request.getVerificationBuffer();
    try {
      mContext.verifyMIC(inToken, 0, inToken.length, inMsg, 0, inMsg.length, msgProp);
    } catch(GSSException ex) {
      throw new RPCAuthException(RPC_AUTH_STATUS_GSS_CREDPROBLEM, ex);
    }
    msgProp = new MessageProp(true);
    try {
      RPCBuffer buffer = new RPCBuffer(mContext.unwrap(data, 0, data.length, msgProp));
      int expected = buffer.readUint32();
      if(expected != creds.getSequenceNum()) {
        LOGGER.warn("Sequence numbers did not match " + expected + " "  + creds.getSequenceNum());
        throw new RPCAcceptedException(RPC_ACCEPT_GARBAGE_ARGS);        
      }
      mSequenceNumbers.add(expected);
      if(mSequenceNumbers.size() > RPCSEC_GSS_SEQUENCE_WINDOW) {
        int first = mSequenceNumbers.first();
        mSequenceNumbers.remove(first);
      }
      return buffer;
    } catch(GSSException ex) {
      throw new RPCAcceptedException(RPC_ACCEPT_GARBAGE_ARGS);
    }
  }
  @Override
  public synchronized byte[] wrap(RPCRequest request, MessageBase response) 
      throws RPCException {
    CredentialsGSS creds = (CredentialsGSS)request.getCredentials();
    Preconditions.checkState(creds.getProcedure() == RPCSEC_GSS_DATA);
    Preconditions.checkState(creds.getService()== RPCSEC_GSS_SERVICE_PRIVACY, 
        String.valueOf(creds.getService()));
    RPCBuffer buffer = new RPCBuffer();
    buffer.writeUint32(creds.getSequenceNum());
    response.write(buffer);
    buffer.flip();
    byte[] data = buffer.readBytes(buffer.length());
    try {
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("Wrapping request for " + mContext.getSrcName());
      }
      return mContext.wrap(data, 0, data.length, new MessageProp(true));
    } catch(GSSException ex) {
      // TODO according to the RFC this should result in no response to the client
      throw new RPCAcceptedException(RPC_ACCEPT_SYSTEM_ERR);
    }
  }
}
