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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_CLID_INUSE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Callback;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETCLIENTIDRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETCLIENTIDResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.Client;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.ClientFactory;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class SETCLIENTIDHandler extends OperationRequestHandler<SETCLIENTIDRequest, SETCLIENTIDResponse> {

  protected static final Logger LOGGER = Logger.getLogger(SETCLIENTIDHandler.class);
  protected static final AtomicLong VERIFER = new AtomicLong(0);

  @Override
  protected SETCLIENTIDResponse doHandle(HDFSState hdfsState, Session session,
      SETCLIENTIDRequest request) throws NFS4Exception {
    /*
     * TODO should follow RFC 3530 page ~211
     */
    ClientID clientID = checkNotNull(request.getClientID(), "clientid");
    Callback callback = checkNotNull(request.getCallback(), "callback");
    ClientFactory clientFactory = hdfsState.getClientFactory();
    Client client = clientFactory.createIfNotExist(clientID);
    String clientHost = session.getClientAddress().getCanonicalHostName();
    if (client == null) {
      client = checkNotNull(clientFactory.get(clientID.getOpaqueID()), "client should exist");
      if (!clientHost.equals(client.getClientHost())) {
        throw new NFS4Exception(NFS4ERR_CLID_INUSE,
            "Session is '" + clientHost + "' and client is '" + client.getClientHost() + "'");
      }
      // update callback info below and client verifer here
      client.getClientID().setVerifer(clientID.getVerifer());
    }
    // server verifer
    OpaqueData8 verifer = new OpaqueData8();
    verifer.setData(Bytes.toBytes(VERIFER.addAndGet(10)));
    client.setCallback(callback);
    client.setVerifer(verifer);
    client.setClientHost(clientHost);
    SETCLIENTIDResponse response = createResponse();
    response.setClientID(client.getShorthandID());
    response.setVerifer(verifer);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected SETCLIENTIDResponse createResponse() {
    return new SETCLIENTIDResponse();
  }
}
