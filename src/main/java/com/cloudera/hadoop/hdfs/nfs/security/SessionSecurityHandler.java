/**
 * Copyright 2012 Cloudera Inc.
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

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;

public abstract class SessionSecurityHandler<VERIFER extends Verifier> {


  public abstract String getUser() throws NFS4Exception;

  public abstract boolean shouldSilentlyDrop(RPCRequest request);


  public abstract VERIFER getVerifer(RPCRequest request) throws RPCException;

  public boolean isUnwrapRequired() {
    return false;
  }

  public boolean isWrapRequired() {
    return false;
  }

  public RPCBuffer unwrap(RPCRequest request, byte[] data ) throws RPCException {
    throw new UnsupportedOperationException();
  }
  public byte[] wrap(RPCRequest request, MessageBase response) throws RPCException {
    throw new UnsupportedOperationException();
  }
}
