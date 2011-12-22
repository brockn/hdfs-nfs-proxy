/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETATTRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETATTRResponse;
import com.google.common.collect.ImmutableMap;



public class SETATTRHandler extends OperationRequestHandler<SETATTRRequest, SETATTRResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(SETATTRHandler.class);

  @Override
  protected SETATTRResponse doHandle(NFS4Handler server, Session session,
      SETATTRRequest request) throws NFS4Exception, IOException {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    Path path = server.getPath(session.getCurrentFileHandle());
    FileSystem fs = session.getFileSystem();
    FileStatus fileStatus = fs.getFileStatus(path);
    ImmutableMap<Integer, Attribute> requestAttrs = request.getAttrValues();
    Bitmap responseAttrs = Attribute.setAttrs(server, session, 
        request.getAttrs(), requestAttrs, fs, fileStatus, request.getStateID());
    SETATTRResponse response = createResponse();
    response.setStatus(NFS4_OK);
    response.setAttrs(responseAttrs);
    return response;
  }
    @Override
  protected SETATTRResponse createResponse() {
    return new SETATTRResponse();
  }
}
