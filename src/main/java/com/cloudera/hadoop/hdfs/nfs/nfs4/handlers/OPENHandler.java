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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.ChangeInfo;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.ChangeID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OPENRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OPENResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSOutputStream;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.google.common.base.Strings;

public class OPENHandler extends OperationRequestHandler<OPENRequest, OPENResponse> {

  protected static final Logger LOGGER = Logger.getLogger(OPENHandler.class);

  @Override
  protected OPENResponse createResponse() {
    return new OPENResponse();
  }

  @Override
  protected OPENResponse doHandle(HDFSState hdfsState, Session session,
      OPENRequest request) throws NFS4Exception, IOException, UnsupportedOperationException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    if (Strings.isNullOrEmpty(request.getName())) {
      throw new NFS4Exception(NFS4ERR_INVAL);
    }
    switch (request.getAccess()) {
      case NFS4_OPEN4_SHARE_ACCESS_READ:
        return read(hdfsState, session, request);
      case NFS4_OPEN4_SHARE_ACCESS_WRITE:
        return write(hdfsState, session, request);
      default:
        throw new NFS4Exception(NFS4ERR_NOTSUPP, "read OR write not both ", true);
    }
  }

  protected OPENResponse read(HDFSState hdfsState, Session session,
      OPENRequest request) throws NFS4Exception, IOException {
    if (request.getDeny() != 0) {
      throw new UnsupportedOperationException("Read access does not support deny " + request.getDeny());
    }
    // generate stateid
    StateID stateID = StateID.newStateID(request.getSeqID());
    FileSystem fs = session.getFileSystem();
    Path parentPath = hdfsState.getPath(session.getCurrentFileHandle());
    Path path = new Path(parentPath, request.getName());
    session.setCurrentFileHandle(hdfsState.getOrCreateFileHandle(path));
    // creates input stream
    hdfsState.openForRead(session.getFileSystem(), stateID, session.getCurrentFileHandle());
    OPENResponse response = createResponse();
    response.setStateID(stateID);
    FileStatus fileStatus = fs.getFileStatus(path);
    // TODO this is  wrong but files in HDFS are currently immutable once closed
    ChangeID changeID = new ChangeID();
    changeID.setChangeID(fileStatus.getModificationTime());
    ChangeInfo changeInfo = new ChangeInfo();
    changeInfo.setChangeIDBefore(changeID);
    changeInfo.setChangeIDAfter(changeID);
    changeInfo.setAtomic(true);
    response.setChangeID(changeInfo);
    response.setResultFlags(NFS4_OPEN4_RESULT_CONFIRM);
    response.setDelgationType(NFS4_CLAIM_NULL);
    response.setStatus(NFS4_OK);
    return response;
  }

  protected OPENResponse write(HDFSState hdfsState, Session session,
      OPENRequest request) throws NFS4Exception, IOException {
    // generate stateid
    StateID stateID = StateID.newStateID(request.getSeqID());
    Path parentPath = hdfsState.getPath(session.getCurrentFileHandle());
    Path path = new Path(parentPath, request.getName());
    session.setCurrentFileHandle(hdfsState.getOrCreateFileHandle(path));
    boolean overwrite = request.getOpenType() == NFS4_OPEN4_CREATE;
    HDFSOutputStream out = hdfsState.openForWrite(session.getFileSystem(), stateID, 
        session.getCurrentFileHandle(), overwrite);
    out.sync(); // create file in namenode
    LOGGER.info(session.getSessionID() + " Opened " + path + " for write " + out);
    OPENResponse response = createResponse();
    response.setStateID(stateID);
    // TODO this is wrong but files in HDFS are currently immutable once closed
    ChangeID changeID = new ChangeID();
    changeID.setChangeID(System.currentTimeMillis());
    ChangeInfo changeInfo = new ChangeInfo();
    changeInfo.setChangeIDBefore(changeID);
    changeInfo.setChangeIDAfter(changeID);
    changeInfo.setAtomic(true);
    response.setChangeID(changeInfo);
    response.setResultFlags(NFS4_OPEN4_RESULT_CONFIRM);
    // the request below is SETATTR returning what was actually set
    if (request.getAttrs() != null) {
      //      Attribute.setAttrs(server, session, request.getAttrs(), request.getAttrValues(), fs, fileStatus, stateID);
      //      Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.getAttrs(server, session, request.getAttrs(), fs, fileStatus);
      //      response.setAttrs(pair.getFirst());
    }
    response.setDelgationType(NFS4_CLAIM_NULL);
    response.setStatus(NFS4_OK);
    return response;
  }
}
