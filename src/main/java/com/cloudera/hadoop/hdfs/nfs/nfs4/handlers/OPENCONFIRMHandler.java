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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OPENCONFIRMRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OPENCONFIRMResponse;

public class OPENCONFIRMHandler extends OperationRequestHandler<OPENCONFIRMRequest, OPENCONFIRMResponse> {

  protected static final Logger LOGGER = Logger.getLogger(OPENCONFIRMHandler.class);

  @Override
  protected OPENCONFIRMResponse doHandle(NFS4Handler server, Session session,
      OPENCONFIRMRequest request) throws NFS4Exception {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    StateID stateID = server.confirm(request.getStateID(), request.getSeqID(), session.getCurrentFileHandle());
    OPENCONFIRMResponse response = createResponse();
    response.setStateID(stateID);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected OPENCONFIRMResponse createResponse() {
    return new OPENCONFIRMResponse();
  }
}
