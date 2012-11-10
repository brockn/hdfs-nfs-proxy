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
package com.cloudera.hadoop.hdfs.nfs.rpc;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

public class RPCAuthException extends RPCDeniedException {
  private static final long serialVersionUID = 5699717505798592024L;
  private final int authState;
  public RPCAuthException(int authState) {
    super(RPC_REJECT_AUTH_ERROR);
    this.authState =  authState;
  }
  public RPCAuthException(int authState, Throwable t) {
    super(RPC_REJECT_AUTH_ERROR, t);
    this.authState =  authState;
  }
  public int getAuthState() {
    return authState;
  }
  @Override
  public String getMessage() {
    String msg = "replyState=" + getReplyState() + 
        ", acceptState=" + getAcceptState() +
        ", authState=" + authState;
    return msg;
  }
}
