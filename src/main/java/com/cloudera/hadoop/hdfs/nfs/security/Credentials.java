package com.cloudera.hadoop.hdfs.nfs.security;


import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;

/**
 * Base class all credentials must extend. 
 */
public abstract class Credentials implements MessageBase {
  
  public Credentials() {
    
  }
  protected static final Logger LOGGER = LoggerFactory.getLogger(Credentials.class);
  protected static final String HOSTNAME;
  protected int mVerifierFlavor, mVeriferLength;
  protected int mCredentialsLength;

  
  static {
    try {
      String s = InetAddress.getLocalHost().getHostName();
      HOSTNAME = s;
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("HOSTNAME = " + HOSTNAME);
      }
    } catch (UnknownHostException e) {
      LOGGER.error("Error setting HOSTNAME", e);
      throw new RuntimeException(e);
    }
  }
  public abstract int getCredentialsFlavor();
  
  
  public abstract String getUsername(Configuration conf)  throws Exception;
  
}
