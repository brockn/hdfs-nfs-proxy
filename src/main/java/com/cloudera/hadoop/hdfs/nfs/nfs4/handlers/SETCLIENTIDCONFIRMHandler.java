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
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_STALE_CLIENTID;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETCLIENTIDCONFIRMRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETCLIENTIDCONFIRMResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.Client;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.ClientFactory;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class SETCLIENTIDCONFIRMHandler extends OperationRequestHandler<SETCLIENTIDCONFIRMRequest, SETCLIENTIDCONFIRMResponse> {

  protected static final Logger LOGGER = Logger.getLogger(SETCLIENTIDCONFIRMHandler.class);

  @Override
  protected SETCLIENTIDCONFIRMResponse doHandle(HDFSState hdfsState, Session session,
      SETCLIENTIDCONFIRMRequest request) throws NFS4Exception {
    /*
     * TODO should follow RFC 3530 page ~215
     */
    ClientFactory clientFactory = hdfsState.getClientFactory();
    Client client = clientFactory.getByShortHand(request.getClientID());
    if (client == null) {
      throw new NFS4Exception(NFS4ERR_STALE_CLIENTID);
    }
    OpaqueData8 verifer = checkNotNull(request.getVerifer(), "verifer");
    if (!verifer.equals(client.getVerifer())) {
      throw new NFS4Exception(NFS4ERR_CLID_INUSE);
    }
    SETCLIENTIDCONFIRMResponse response = createResponse();
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected SETCLIENTIDCONFIRMResponse createResponse() {
    return new SETCLIENTIDCONFIRMResponse();
  }
}
