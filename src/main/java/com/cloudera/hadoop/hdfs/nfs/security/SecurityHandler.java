package com.cloudera.hadoop.hdfs.nfs.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.ietf.jgss.GSSException;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
public class SecurityHandler {
  protected static final Logger LOGGER = Logger.getLogger(SecurityHandler.class);
  public static SecurityHandler getInstance(Configuration conf) {
    try {
      if("kerberos".equals(conf.get("hadoop.security.authentication"))) {
        return new GSSSecurityHandler();
      }
      return new SecurityHandler();
    } catch (GSSException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean hasAcceptableSecurity(RPCRequest request) {
    if(request.getCredentials() != null && request.getVerifier() != null) {
      return request.getCredentials() instanceof CredentialsSystem && request.getVerifier() instanceof VerifierNone;
    }
    return false;
  }

  public Pair<? extends Verifier, RPCBuffer> initializeContext(RPCRequest request, RPCBuffer buffer) throws NFS4Exception {
    return Pair.of(new VerifierNone(), null);
  }

  public Verifier getVerifer(RPCRequest request) throws NFS4Exception {
    return new VerifierNone();
  }

  public boolean isWrapRequired() {
    return false;
  }

  public boolean isUnwrapRequired() {
    return false;
  }

  public byte[] unwrap(byte[] data ) throws NFS4Exception {
    throw new UnsupportedOperationException();
  }
  public byte[] wrap(MessageBase response) throws NFS4Exception {
    throw new UnsupportedOperationException();
  }
}
