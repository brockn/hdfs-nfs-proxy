package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

/**
 * Implementation of AUTH_NONE
 */
public class CredentialsNone extends Credentials {
  
  public CredentialsNone() {
    super();
    this.mCredentialsLength = 0;
    this.mVerifierFlavor = RPC_VERIFIER_NULL;
    this.mVeriferLength = 0;
  }

  protected static final Logger LOGGER = LoggerFactory.getLogger(CredentialsNone.class);

  @Override
  public void read(RPCBuffer buffer) {
    mCredentialsLength = buffer.readInt();
    if(mCredentialsLength != 0) {
      throw new RuntimeException("Length of " + this + " should be 0");
    }
    buffer.skip(mCredentialsLength);
    /*
     * CredentialsNone so we have nothing to read. 
     */
    mVerifierFlavor = buffer.readInt();
    mVeriferLength = buffer.readInt();
    if(mVeriferLength > RPC_OPAQUE_AUTH_MAX) {
      throw new RuntimeException("veriferLength too large " + mVeriferLength);
    }
    buffer.skip(mVeriferLength);
  }

  @Override
  public void write(RPCBuffer buffer) {
    if(mCredentialsLength != 0) {
      throw new RuntimeException("Length of " + this + " should be 0");
    }
    buffer.writeInt(mCredentialsLength);
    buffer.writeInt(mVerifierFlavor);
    buffer.writeInt(mVeriferLength);
  }

  @Override
  public int getCredentialsFlavor() {
    return RPC_AUTH_NULL;
  }

  @Override
  public String toString() {
    return "CredentialsNull [mVerifierFlavor=" + mVerifierFlavor
        + ", mVeriferLength=" + mVeriferLength + ", mCredentialsLength="
        + mCredentialsLength + ", getCredentialsFlavor()="
        + getCredentialsFlavor() + ", getClass()=" + getClass()
        + ", hashCode()=" + hashCode() + ", toString()=" + super.toString()
        + "]";
  }
  public String getUsername(Configuration conf) throws Exception{
    return ANONYMOUS_USERNAME;
  }

 }
