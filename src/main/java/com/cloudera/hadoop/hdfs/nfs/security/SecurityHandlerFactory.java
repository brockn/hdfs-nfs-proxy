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

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.ietf.jgss.GSSManager;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.UserIDMapper;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCAuthException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

public class SecurityHandlerFactory {  
  private static final Logger LOGGER = Logger.getLogger(SecurityHandlerFactory.class);
  private final Configuration mConfiguration;
  private final Supplier<GSSManager> mGSSManagerSupplier;
  private final ClientHostsMatcher mClientHostsMatcher;
  private final boolean mKerberosEnabled;
  private final AtomicInteger mContextIDGenerator;  
  private final Map<ContextID, SessionSecurityHandlerGSS> mHandlers;
  private final SessionSecurityHandlerGSSFactory mSessionSecurityHandlerGSSFactory;

  public SecurityHandlerFactory(Configuration configuration, 
      Supplier<GSSManager> gssManagerSupplier, 
      SessionSecurityHandlerGSSFactory sessionSecurityHandlerGSSFactory) {
    mConfiguration = configuration;
    mGSSManagerSupplier = gssManagerSupplier;
    mSessionSecurityHandlerGSSFactory = sessionSecurityHandlerGSSFactory;
    
    String allowedHosts = mConfiguration.get(SECURITY_ALLOWED_HOSTS, "").trim();
    if(allowedHosts.isEmpty()) {
      throw new IllegalArgumentException("Required argument not found " + SECURITY_ALLOWED_HOSTS);
    }    
    mClientHostsMatcher = new ClientHostsMatcher(allowedHosts);  
    String securityFlavor = mConfiguration.get(SECURITY_FLAVOR, "").trim();
    if(SECURITY_FLAVOR_KERBEROS.equalsIgnoreCase(securityFlavor)) {
      mKerberosEnabled = true;
    } else if(SECURITY_FLAVOR_DEFAULT.equalsIgnoreCase(securityFlavor)) {
      mKerberosEnabled = false;
    } else {
      throw new IllegalArgumentException("Unknown security flavor '" + securityFlavor + "'");
    }
    mHandlers = Maps.newConcurrentMap();
    mContextIDGenerator = new AtomicInteger(0);
  }
  
  public SessionSecurityHandler<? extends Verifier> getSecurityHandler(Credentials credentials)
  throws RPCException {
    boolean hasSecureCredentials = credentials instanceof CredentialsGSS;
    if(mKerberosEnabled) {
      if(!hasSecureCredentials) {
        throw new RPCAuthException(RPC_AUTH_STATUS_BADCRED);
      }
      CredentialsGSS secureCredentials = (CredentialsGSS)credentials;
      Preconditions.checkState(secureCredentials.getProcedure() == RPCSEC_GSS_DATA, 
          String.valueOf(secureCredentials.getProcedure()));
      // TODO support none and integrity
      Preconditions.checkState(secureCredentials.getService() == RPCSEC_GSS_SERVICE_PRIVACY, 
          String.valueOf(secureCredentials.getService()));
      ContextID contextID = new ContextID(secureCredentials.getContext().getData());
      SessionSecurityHandlerGSS securityHandler = mHandlers.get(contextID);
      if(securityHandler == null) {
        LOGGER.warn("Unable to find ContextID " + Bytes.asHex(contextID.contextID));
        throw new RPCAuthException(RPC_AUTH_STATUS_GSS_CREDPROBLEM);
      }
      return securityHandler;
    }
    if(hasSecureCredentials) {
      throw new RPCAuthException(RPC_AUTH_STATUS_REJECTEDCRED);
    }
    return new SessionSecurityHandlerSystem((CredentialsSystem)credentials,
        UserIDMapper.get(mConfiguration));
  }
  
  public boolean isClientAllowed(InetAddress addr) {
    return mClientHostsMatcher.isIncluded(addr.getHostAddress(), 
        addr.getCanonicalHostName());
  }
  public RPCBuffer handleNullRequest(String sessionID, String clientName, 
      RPCRequest request, RPCBuffer buffer) throws RPCException {
    Credentials creds = request.getCredentials();
    boolean hasSecureCredentials = creds instanceof CredentialsGSS;
    if(!hasSecureCredentials) {
      RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
      response.setReplyState(RPC_REPLY_STATE_ACCEPT);
      response.setAcceptState(RPC_ACCEPT_SUCCESS);
      response.setVerifier(new VerifierNone());
      RPCBuffer responseBuffer = new RPCBuffer();
      // save space for length
      responseBuffer.writeInt(Integer.MAX_VALUE);
      response.write(responseBuffer);
      responseBuffer.flip();
      return responseBuffer;
    }
    if(!mKerberosEnabled) {
      throw new RPCAuthException(RPC_AUTH_STATUS_BADCRED);
    }
    CredentialsGSS credentials = (CredentialsGSS)request.getCredentials();
    if(credentials.getProcedure() == RPCSEC_GSS_INIT || 
        credentials.getProcedure() == RPCSEC_GSS_CONTINUE_INIT) {
      SessionSecurityHandlerGSS securityHandler = mSessionSecurityHandlerGSSFactory.
          getInstance(credentials, mGSSManagerSupplier.get(), 
              mContextIDGenerator.incrementAndGet(), 
              mConfiguration.get(SUPER_USER, DEFAULT_SUPER_USER));
      ContextID contextID = new ContextID(securityHandler.getContextID());
      LOGGER.debug("Created contextID " + Bytes.asHex(contextID.contextID));
      mHandlers.put(contextID, securityHandler);
      Pair<? extends Verifier, InitializeResponse> security = securityHandler.initializeContext(request, buffer);
      LOGGER.info(sessionID + " Handling NFS NULL for GSS Init Procedure for " + clientName);
      RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
      response.setReplyState(RPC_REPLY_STATE_ACCEPT);
      response.setAcceptState(RPC_ACCEPT_SUCCESS);
      response.setVerifier(security.getFirst());
      
      RPCBuffer responseBuffer = new RPCBuffer();
      // save space for length
      responseBuffer.writeInt(Integer.MAX_VALUE);
      response.write(responseBuffer);
      InitializeResponse initResponse = security.getSecond();
      initResponse.write(responseBuffer);
      responseBuffer.flip();
      return responseBuffer;
    } else if(credentials.getProcedure() == RPCSEC_GSS_DESTROY) {
      // TODO what to do here?
      @SuppressWarnings("unused")
      SessionSecurityHandlerGSS securityHandler = mHandlers.get(new ContextID(credentials.getContext().getData()));
      LOGGER.info(sessionID + " Handling NFS NULL for GSS Destroy Procedure for " + clientName);
      RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
      response.setReplyState(RPC_REPLY_STATE_ACCEPT);
      response.setAcceptState(RPC_ACCEPT_SUCCESS);
      response.setVerifier(new VerifierNone());
      RPCBuffer responseBuffer = new RPCBuffer();
      // save space for length
      responseBuffer.writeInt(Integer.MAX_VALUE);
      response.write(responseBuffer);
      responseBuffer.flip();
      return responseBuffer;
    }
    throw new RPCAuthException(RPC_AUTH_STATUS_BADCRED);
  }
  private static class ContextID {
    private final byte[] contextID;
    public ContextID(byte[] contextID) {
     this.contextID = contextID;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ContextID other = (ContextID) obj;
      if (!Arrays.equals(contextID, other.contextID))
        return false;
      return true;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(contextID);
      return result;
    }
  }
}
