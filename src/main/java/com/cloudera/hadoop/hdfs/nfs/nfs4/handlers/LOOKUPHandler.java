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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_INVAL;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_NOENT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_NOFILEHANDLE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.LOOKUPRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.LOOKUPResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class LOOKUPHandler extends OperationRequestHandler<LOOKUPRequest, LOOKUPResponse> {

  protected static final Logger LOGGER = Logger.getLogger(LOOKUPHandler.class);

  @Override
  protected LOOKUPResponse doHandle(HDFSState hdfsState, Session session,
      LOOKUPRequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    if ("".equals(request.getName())) {
      throw new NFS4Exception(NFS4ERR_INVAL);
    }
    Path parentPath = hdfsState.getPath(session.getCurrentFileHandle());
    Path path = new Path(parentPath, request.getName());
    FileSystem fs = session.getFileSystem();
    if (!hdfsState.fileExists(fs, path)) {
      throw new NFS4Exception(NFS4ERR_NOENT, "Path " + path + " does not exist.", true);
    }
    LOOKUPResponse response = createResponse();
    FileHandle fileHandle = hdfsState.createFileHandle(path);
    session.setCurrentFileHandle(fileHandle);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected LOOKUPResponse createResponse() {
    return new LOOKUPResponse();
  }
}
