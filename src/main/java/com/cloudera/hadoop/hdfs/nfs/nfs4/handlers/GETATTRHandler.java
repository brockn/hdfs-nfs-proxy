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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.GETATTRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.GETATTRResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.google.common.collect.ImmutableList;

public class GETATTRHandler extends OperationRequestHandler<GETATTRRequest, GETATTRResponse> {

  protected static final Logger LOGGER = Logger.getLogger(GETATTRHandler.class);

  @Override
  protected GETATTRResponse doHandle(HDFSState hdfsState, Session session,
      GETATTRRequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    Path path = hdfsState.getPath(session.getCurrentFileHandle());
    try {
      FileSystem fs = session.getFileSystem();
      FileStatus fileStatus = fs.getFileStatus(path);
      Pair<Bitmap, ImmutableList<Attribute>> attrs = Attribute.getAttrs(hdfsState, session,
          request.getAttrs(), fs, fileStatus);
      GETATTRResponse response = createResponse();
      response.setStatus(NFS4_OK);
      response.setAttrs(attrs.getFirst());
      response.setAttrValues(attrs.getSecond());
      return response;
    } catch (FileNotFoundException e) {
      throw new NFS4Exception(NFS4ERR_NOENT);
    }
  }

  @Override
  protected GETATTRResponse createResponse() {
    return new GETATTRResponse();
  }
}
