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
package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS_PROC_COMPOUND;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS_PROG;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS_VERSION;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_ACCEPT_STATE_ACCEPT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_AUTH_NULL;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_MESSAGE_TYPE_CALL;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_MESSAGE_TYPE_REPLY;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_REJECT_AUTH_ERROR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_REPLY_STATE_ACCEPT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_VERSION;
import static org.junit.Assert.assertEquals;

import com.cloudera.hadoop.hdfs.nfs.security.CredentialsNone;
import com.cloudera.hadoop.hdfs.nfs.security.VerifierNone;


public class RPCTestUtil {
  public static RPCRequest createRequest() {
    int xid = XID.next();
    RPCRequest base = new RPCRequest(xid, RPC_VERSION);
    base.setProgram(NFS_PROG);
    base.setProcedure(NFS_PROC_COMPOUND);
    base.setProgramVersion(NFS_VERSION);
    assertEquals(xid, base.getXid());
    assertEquals(RPC_MESSAGE_TYPE_CALL, base.getMessageType());
    assertEquals(RPC_VERSION, base.getRpcVersion());
    assertEquals(NFS_PROG, base.getProgram());
    assertEquals(NFS_VERSION, base.getProgramVersion());
    assertEquals(NFS_PROC_COMPOUND, base.getProcedure());
    base.setCredentials(new CredentialsNone());
    base.setVerifier(new VerifierNone());
    assertEquals(RPC_AUTH_NULL, base.getCredentials().getFlavor());
    return base;
  }

  public static RPCResponse createResponse() {
    int xid = XID.next();
    RPCResponse response = new RPCResponse(xid, RPC_VERSION);
    response.setProgram(NFS_PROG);
    response.setProcedure(NFS_PROC_COMPOUND);
    response.setProgramVersion(NFS_VERSION);
    response.setReplyState(RPC_REPLY_STATE_ACCEPT);
    response.setAcceptState(RPC_ACCEPT_STATE_ACCEPT);
    response.setAuthState(RPC_REJECT_AUTH_ERROR);
    response.setVerifier(new VerifierNone());

    assertEquals(xid, response.getXid());
    assertEquals(RPC_MESSAGE_TYPE_REPLY, response.getMessageType());
    assertEquals(RPC_VERSION, response.getRpcVersion());
    assertEquals(NFS_PROG, response.getProgram());
    assertEquals(NFS_VERSION, response.getProgramVersion());
    assertEquals(NFS_PROC_COMPOUND, response.getProcedure());
    assertEquals(RPC_REPLY_STATE_ACCEPT, response.getReplyState());
    assertEquals(RPC_ACCEPT_STATE_ACCEPT, response.getAcceptState());
    assertEquals(RPC_REJECT_AUTH_ERROR, response.getAuthState());

    return response;
  }
}
