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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_EXIST;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_FILE_OPEN;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_INVAL;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_IO;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_NOENT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_NOFILEHANDLE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_NOTDIR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.ChangeInfo;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RENAMERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.RENAMEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class RENAMEHandler extends OperationRequestHandler<RENAMERequest, RENAMEResponse> {

  protected static final Logger LOGGER = Logger.getLogger(RENAMEHandler.class);

  @Override
  protected RENAMEResponse doHandle(HDFSState hdfsState, Session session,
      RENAMERequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null || session.getSavedFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    if ("".equals(request.getOldName()) || "".equals(request.getNewName())) {
      throw new NFS4Exception(NFS4ERR_INVAL);
    }
    FileSystem fs = session.getFileSystem();
    Path oldParentPath = hdfsState.getPath(session.getSavedFileHandle());
    Path oldPath = new Path(oldParentPath, request.getOldName());
    Path newParentPath = hdfsState.getPath(session.getCurrentFileHandle());
    Path newPath = new Path(newParentPath, request.getNewName());
    if (!(fs.getFileStatus(oldParentPath).isDir() && fs.getFileStatus(newParentPath).isDir())) {
      throw new NFS4Exception(NFS4ERR_NOTDIR);
    }
    if (!hdfsState.fileExists(fs, oldPath)) {
      throw new NFS4Exception(NFS4ERR_NOENT, "Path " + oldPath + " does not exist.");
    }
    if (hdfsState.fileExists(fs, newPath)) {
      // TODO according to the RFC we are supposed to check to see if
      // the entry which exists is compatible (overwrite file and
      // empty directory if the "old" item is a file or dir respectively.
      throw new NFS4Exception(NFS4ERR_EXIST, "Path " + newPath + " exists.");
    }
    // we won't support renaming files which are open
    // but it happens fairly often that due to tcp/ip
    // the rename request is received before the close.
    // Below we delay the rename if the file is open
    for (int i = 0; i < 5; i++) {
      if (!hdfsState.isFileOpen(oldPath)) {
        break;
      }
      try {
        Thread.sleep(100L);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for file to close", e);
      }
    }
    if (hdfsState.isFileOpen(oldPath)) {
      throw new NFS4Exception(NFS4ERR_FILE_OPEN);
    }
    LOGGER.info(session.getSessionID() + " Renaming " + oldPath + " to " + newPath);
    long beforeSource = fs.getFileStatus(oldParentPath).getModificationTime();
    long beforeDest = fs.getFileStatus(newParentPath).getModificationTime();
    if (!fs.rename(oldPath, newPath)) {
      throw new NFS4Exception(NFS4ERR_IO);
    }
    long afterSource = fs.getFileStatus(oldParentPath).getModificationTime();
    long afterDest = fs.getFileStatus(newParentPath).getModificationTime();
    RENAMEResponse response = createResponse();
    response.setChangeInfoSource(ChangeInfo.newChangeInfo(true, beforeSource, afterSource));
    response.setChangeInfoDest(ChangeInfo.newChangeInfo(true, beforeDest, afterDest));
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected RENAMEResponse createResponse() {
    return new RENAMEResponse();
  }
}
