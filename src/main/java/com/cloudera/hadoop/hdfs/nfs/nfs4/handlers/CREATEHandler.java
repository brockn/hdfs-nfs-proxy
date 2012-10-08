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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_EXIST;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_INVAL;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_IO;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_NOFILEHANDLE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_STALE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_DIR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ChangeInfo;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CREATERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CREATEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

public class CREATEHandler extends OperationRequestHandler<CREATERequest, CREATEResponse> {

  protected static final Logger LOGGER = Logger.getLogger(CREATEHandler.class);

  @Override
  protected CREATEResponse doHandle(HDFSState hdfsState, Session session,
      CREATERequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    if (request.getType() != NFS4_DIR) {
      throw new UnsupportedOperationException("Create files of  type " + request.getType() + " is not supported.");
    }
    if (Strings.isNullOrEmpty(request.getName())) {
      throw new NFS4Exception(NFS4ERR_INVAL);
    }
    Path parent = hdfsState.getPath(session.getCurrentFileHandle());
    Path path = new Path(parent, request.getName());
    FileSystem fs = session.getFileSystem();
    if (!fs.exists(parent)) {
      throw new NFS4Exception(NFS4ERR_STALE, "Parent " + parent + " does not exist");
    }
    if (fs.exists(path)) {
      throw new NFS4Exception(NFS4ERR_EXIST, "Path " + path + " already exists.");
    }
    long parentModTimeBefore = fs.getFileStatus(parent).getModificationTime();
    if (!fs.mkdirs(path)) {
      throw new NFS4Exception(NFS4ERR_IO);
    }
    long parentModTimeAfter = fs.getFileStatus(parent).getModificationTime();
    FileStatus fileStatus = fs.getFileStatus(path);
    ImmutableMap<Integer, Attribute> requestAttrs = request.getAttrValues();
    // TODO Handlers should have annotations so that setAttrs can throw an
    // error if they require the stateID to be set.
    Bitmap responseAttrs = Attribute.setAttrs(hdfsState, session,
        request.getAttrs(), requestAttrs, fs, fileStatus, null);
    session.setCurrentFileHandle(hdfsState.createFileHandle(path));
    CREATEResponse response = createResponse();
    response.setChangeInfo(ChangeInfo.newChangeInfo(true, parentModTimeBefore, parentModTimeAfter));
    response.setStatus(NFS4_OK);
    response.setAttrs(responseAttrs);
    return response;
  }

  @Override
  protected CREATEResponse createResponse() {
    return new CREATEResponse();
  }
}
