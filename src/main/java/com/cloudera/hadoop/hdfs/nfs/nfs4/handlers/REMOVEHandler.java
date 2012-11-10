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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.ChangeInfo;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.ChangeID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.REMOVERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.REMOVEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.google.common.base.Strings;

public class REMOVEHandler extends OperationRequestHandler<REMOVERequest, REMOVEResponse> {

  protected static final Logger LOGGER = Logger.getLogger(REMOVEHandler.class);

  @Override
  protected REMOVEResponse createResponse() {
    return new REMOVEResponse();
  }

  @Override
  protected REMOVEResponse doHandle(HDFSState hdfsState, Session session,
      REMOVERequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    if (Strings.isNullOrEmpty(request.getName())) {
      throw new NFS4Exception(NFS4ERR_INVAL);
    }
    Path parentPath = hdfsState.getPath(session.getCurrentFileHandle());
    Path path = new Path(parentPath, request.getName());
    FileSystem fs = session.getFileSystem();
    
    if (!hdfsState.fileExists(session.getFileSystem(), path)) {
      throw new NFS4Exception(NFS4ERR_NOENT);
    }
    REMOVEResponse response = createResponse();
    ChangeInfo changeInfo = new ChangeInfo();
    FileStatus parentStatus = fs.getFileStatus(parentPath);
    ChangeID changeIDBefore = new ChangeID();
    changeIDBefore.setChangeID(parentStatus.getModificationTime());
    changeInfo.setChangeIDBefore(changeIDBefore);

    // TODO we should handle this better like HDFS does or at least
    // cleanup the write order handlers for files which are open
    // for write. The call below will return false if the file
    // is open for write. Which could be a long time if the 
    // server writing to the file dies.
    if(!hdfsState.delete(session.getFileSystem(), path)) {
      throw new NFS4Exception(NFS4ERR_PERM);
    }

    parentStatus = fs.getFileStatus(parentPath);
    ChangeID changeIDAfter = new ChangeID();
    changeIDAfter.setChangeID(parentStatus.getModificationTime());
    changeInfo.setChangeIDAfter(changeIDAfter);
    changeInfo.setAtomic(true);
    response.setChangeInfo(changeInfo);
    response.setStatus(NFS4_OK);
    return response;
  }
}
