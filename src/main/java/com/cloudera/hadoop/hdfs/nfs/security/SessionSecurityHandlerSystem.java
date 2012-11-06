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

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.UserIDMapper;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
public class SessionSecurityHandlerSystem extends SessionSecurityHandler<VerifierNone> {
  protected static final Logger LOGGER = Logger.getLogger(SessionSecurityHandlerSystem.class);
  private final UserIDMapper mUserIDMapper;
  private final CredentialsSystem mCredentialsSystem;
  public SessionSecurityHandlerSystem(CredentialsSystem credentialsSystem,
      UserIDMapper userIDMapper) {
    mCredentialsSystem = credentialsSystem;
    mUserIDMapper = userIDMapper;
  }
  @Override
  public String getUser() throws NFS4Exception {
    try {
      return mUserIDMapper.getUserForUID(mCredentialsSystem.getUID(), ANONYMOUS_USERNAME); 
    } catch (Exception e) {
      throw new NFS4Exception(NFS4ERR_SERVERFAULT, e);
    }
  }

  @Override
  public boolean shouldSilentlyDrop(RPCRequest request) {
    return false;
  }

  @Override
  public VerifierNone getVerifer(RPCRequest request) throws RPCException {
    return new VerifierNone();
  }
}
