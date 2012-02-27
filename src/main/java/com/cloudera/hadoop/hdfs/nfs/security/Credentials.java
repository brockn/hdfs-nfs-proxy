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
package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

/**
 * Base class all credentials must extend.
 */
public abstract class Credentials implements MessageBase {
  protected static final Logger LOGGER = Logger.getLogger(Credentials.class);

  public Credentials() {

  }
  protected static final String HOSTNAME;
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

  public abstract int getFlavor();


  public abstract String getUsername(Configuration conf)  throws Exception;

  public static Credentials readCredentials(int flavor, RPCBuffer buffer) {
    Credentials credentials;
    if(flavor == RPC_AUTH_NULL) {
      credentials = new CredentialsNone();
    } else if(flavor == RPC_AUTH_UNIX) {
      credentials = new CredentialsSystem();
    } else if(flavor == RPC_AUTH_GSS) {
      credentials = new CredentialsGSS();
    } else {
      throw new UnsupportedOperationException("Unsupported Credentials Flavor " + flavor);
    }
    credentials.read(buffer);
    return credentials;
  }
}
