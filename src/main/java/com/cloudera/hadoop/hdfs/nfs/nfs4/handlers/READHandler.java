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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.READRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.READResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSInputStream;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class READHandler extends OperationRequestHandler<READRequest, READResponse> {

  protected static final Logger LOGGER = Logger.getLogger(READHandler.class);

  @Override
  protected READResponse doHandle(HDFSState hdfsState, Session session,
      READRequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    int size = Math.min(request.getCount(), NFS4_MAX_RWSIZE);
    if (size < 0) {
      throw new NFS4Exception(NFS4ERR_INVAL);
    }
    FileHandle fileHandle = session.getCurrentFileHandle();
    Path path = hdfsState.getPath(fileHandle);
    FileSystem fs = session.getFileSystem();
    HDFSInputStream inputStream = hdfsState.forRead(request.getStateID(), fs, fileHandle);
    synchronized (inputStream) {
      if (inputStream.getPos() != request.getOffset()) {
        try {
          inputStream.seek(request.getOffset());
        } catch (IOException e) {
          throw new IOException(e.getMessage() + ": " + inputStream.getPos() + ", " + request.getOffset(), e);
        }
        hdfsState.incrementMetric("NFS_RANDOM_READS", 1);
      }
      READResponse response = createResponse();
      byte[] data = new byte[size];
      int count = inputStream.read(data);
      long fileLength = -1;
      if (count > 0 && count != data.length
          && (request.getOffset() + count) < (fileLength = fs.getFileStatus(path).getLen())) {
        LOGGER.info("Short read " + path
            + " at pos = " + request.getOffset()
            + ", wanted " + data.length + " and read " + count
            + ", fileLength = " + fileLength);
        hdfsState.incrementMetric("NFS_SHORT_READS", 1);
      }
      boolean eof = count < 0;
      if (eof) {
        data = new byte[0];
        count = 0;
      }
      hdfsState.incrementMetric("HDFS_BYTES_READ", count);
      response.setData(data, 0, count);
      response.setEOF(eof);
      response.setStatus(NFS4_OK);
      return response;
    }
  }

  @Override
  protected READResponse createResponse() {
    return new READResponse();
  }
}
