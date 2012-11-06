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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CLOSERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CLOSEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class CLOSEHandler extends OperationRequestHandler<CLOSERequest, CLOSEResponse> {

  protected static final Logger LOGGER = Logger.getLogger(CLOSEHandler.class);

  @Override
  protected CLOSEResponse createResponse() {
    return new CLOSEResponse();
  }
  @Override
  protected CLOSEResponse doHandle(HDFSState hdfsState, Session session,
      CLOSERequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    StateID stateID = hdfsState.close(session.getSessionID(), request.getStateID(),
        request.getSeqID(), session.getCurrentFileHandle());
    CLOSEResponse response = createResponse();
    response.setStateID(stateID);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  public boolean wouldBlock(HDFSState hdfsState, Session session, CLOSERequest request) {
    try {
      if(session.getCurrentFileHandle() != null) {
        WriteOrderHandler writeOrderHanlder = hdfsState.getWriteOrderHandler(session.getCurrentFileHandle());
        if(writeOrderHanlder != null) {
          return writeOrderHanlder.closeWouldBlock();          
        }
      }
    } catch(IOException e) {
      LOGGER.warn("Expection handing wouldBlock. Client error will " +
          "be returned on call to doHandle", e);
    }
    return false;
  }
}
