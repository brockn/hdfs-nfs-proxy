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

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.COMMITRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.COMMITResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class COMMITHandler extends OperationRequestHandler<COMMITRequest, COMMITResponse> {

  protected static final Logger LOGGER = Logger.getLogger(COMMITHandler.class);
  @Override
  protected COMMITResponse createResponse() {
    return new COMMITResponse();
  }
  @Override
  protected COMMITResponse doHandle(HDFSState hdfsState, Session session,
      COMMITRequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    WriteOrderHandler writeOrderHandler = hdfsState.getWriteOrderHandler(session.getCurrentFileHandle());
    if(writeOrderHandler == null) {
      throw new NFS4Exception(NFS4ERR_STALE);
    }
    long offset = request.getOffset() + request.getCount();
    if (offset == 0) {
      offset = writeOrderHandler.getCurrentPos();
    }
    writeOrderHandler.sync(offset);
    COMMITResponse response = createResponse();
    OpaqueData8 verifer = new OpaqueData8();
    verifer.setData(Bytes.toBytes(hdfsState.getStartTime()));
    response.setVerifer(verifer);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  public boolean wouldBlock(HDFSState hdfsState, Session session, COMMITRequest request) {
    if (session.getCurrentFileHandle() == null) {
      return false;
    }
    WriteOrderHandler writeOrderHandler = hdfsState.getWriteOrderHandler(session.getCurrentFileHandle());
    if(writeOrderHandler == null) {
      return false;
    }
    long offset = request.getOffset() + request.getCount();
    if (offset == 0) {
      offset = writeOrderHandler.getCurrentPos();
    }
    return writeOrderHandler.syncWouldBlock(offset);
  }
}
