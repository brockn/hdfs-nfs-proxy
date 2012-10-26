package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_CONTINUE_INIT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_DATA;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_DESTROY;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_INIT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_ACCEPT_SUCCESS;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_REPLY_STATE_ACCEPT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_VERSION;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.SECURITY_FLAVOR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.SECURITY_FLAVOR_DEFAULT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.SECURITY_FLAVOR_KERBEROS;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.ietf.jgss.GSSException;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class SecurityHandlerFactory {
  private static final Logger LOGGER = Logger.getLogger(SecurityHandlerFactory.class);

  private final Configuration mConfiguration;
  private final boolean mSecurityEnabled;
  private final AtomicInteger mContextIDGenerator;
  private final Map<ContextID, GSSSecurityHandler> mHandlers;
  
  public SecurityHandlerFactory(Configuration configuration) {
    mConfiguration = configuration;
    mSecurityEnabled = SECURITY_FLAVOR_KERBEROS.equalsIgnoreCase(
        mConfiguration.get(SECURITY_FLAVOR));
    mHandlers = Maps.newConcurrentMap();
    mContextIDGenerator = new AtomicInteger(0);
  }

  public RPCResponse handleNullRequest(String sessionID, String clientName, 
      RPCRequest request, RPCBuffer buffer) {
    if(!(request.getCredentials() instanceof CredentialsGSS)) {
      RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
      response.setReplyState(RPC_REPLY_STATE_ACCEPT);
      response.setAcceptState(RPC_ACCEPT_SUCCESS);
      response.setVerifier(new VerifierNone());
      return response;
    }
    CredentialsGSS credentials = (CredentialsGSS)request.getCredentials();
    if(credentials.getProcedure() == RPCSEC_GSS_INIT || 
        credentials.getProcedure() == RPCSEC_GSS_CONTINUE_INIT) {
      int contextID = mContextIDGenerator.incrementAndGet();
      try {
        GSSSecurityHandler securityHandler = new GSSSecurityHandler(contextID);
        mHandlers.put(new ContextID(securityHandler.getContextID()), securityHandler);
        Pair<? extends Verifier, RPCBuffer> security = securityHandler.initializeContext(request, buffer);
        LOGGER.info(sessionID + " Handling NFS NULL for GSS Init Procedure for " + clientName);
        RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
        response.setReplyState(RPC_REPLY_STATE_ACCEPT);
        response.setAcceptState(RPC_ACCEPT_SUCCESS);
        response.setVerifier(security.getFirst());
        return response;        
      } catch (GSSException e) {
        throw new RuntimeException(e);
      }
    } else if(credentials.getProcedure() == RPCSEC_GSS_DESTROY) {
      // TODO what to do here?
      GSSSecurityHandler securityHandler = mHandlers.get(new ContextID(credentials.getContext().getData()));
      LOGGER.info(sessionID + " Handling NFS NULL for GSS Destroy Procedure for " + clientName);
      RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
      response.setReplyState(RPC_REPLY_STATE_ACCEPT);
      response.setAcceptState(RPC_ACCEPT_SUCCESS);
      response.setVerifier(new VerifierNone());
      return response;
    }
    throw new UnsupportedOperationException("Unknown credentials " +
    		"proccedure " + credentials.getProcedure());
  }
  public boolean hasAcceptableSecurity(RPCRequest request) {
    if(request.getCredentials() != null && request.getVerifier() != null) {
     if(request.getCredentials() instanceof CredentialsSystem && 
         request.getVerifier() instanceof VerifierNone) {
       return true;
     }
     if(request.getCredentials() != null && 
         request.getCredentials() instanceof CredentialsGSS) {
       return true;
     }
    }
    return false;
  }
  
  public SecurityHandler getSecurityHandler(Credentials credentials)
  throws NFS4Exception {
    boolean hasSecureCredentials = credentials instanceof CredentialsGSS;
    if(mSecurityEnabled) {
      if(!hasSecureCredentials) {
        throw new NFS4Exception(NFS4ERR_WRONGSEC);
      }
      CredentialsGSS secureCredentials = (CredentialsGSS)credentials;
      Preconditions.checkState(secureCredentials.getProcedure() == RPCSEC_GSS_DATA, 
          String.valueOf(secureCredentials.getProcedure()));
      // TODO support none and integrity
      Preconditions.checkState(secureCredentials.getService() == RPCSEC_GSS_SERVICE_PRIVACY, 
          String.valueOf(secureCredentials.getService()));
      ContextID contextID = new ContextID(secureCredentials.getContext().getData());
      GSSSecurityHandler securityHandler = mHandlers.get(contextID);
      if(securityHandler == null) {
        throw new NFS4Exception(NFS4ERR_PERM);
      }
      return securityHandler;
    }
    if(hasSecureCredentials) {
      throw new NFS4Exception(NFS4ERR_WRONGSEC);
    }
    return new SecurityHandler();
  }
  
  private static class ContextID {
    private final byte[] contextID;
    public ContextID(byte[] contextID) {
     this.contextID = contextID;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(contextID);
      return result;
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
  }
}
