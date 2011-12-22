/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
