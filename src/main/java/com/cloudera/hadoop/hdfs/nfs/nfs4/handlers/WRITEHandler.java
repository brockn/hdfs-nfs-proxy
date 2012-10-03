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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_NOFILEHANDLE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_COMMIT_UNSTABLE4;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.WRITERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.WRITEResponse;

public class WRITEHandler extends OperationRequestHandler<WRITERequest, WRITEResponse> {

  protected static final Logger LOGGER = Logger.getLogger(WRITEHandler.class);
  @Override
  public boolean wouldBlock(NFS4Handler server, Session session, WRITERequest request) {
    try {
      if (session.getCurrentFileHandle() == null) {
        throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
      }
      FileHandle fileHandle = session.getCurrentFileHandle();
      Path path = server.getPath(fileHandle);
      String file = path.toUri().getPath();
      FSDataOutputStream out = server.forWrite(request.getStateID(), session.getFileSystem(), fileHandle, false);
      WriteOrderHandler writeOrderHandler = server.getWriteOrderHandler(file, out);
      boolean sync = request.getStable() != NFS4_COMMIT_UNSTABLE4;
      return writeOrderHandler.writeWouldBlock(request.getOffset(), sync);
    } catch(NFS4Exception e) {
      LOGGER.warn("Expection handing wouldBlock. Client error will " +
          "be returned on call to doHandle", e);
    } catch(IOException e) {
      LOGGER.warn("Expection handing wouldBlock. Client error will " +
          "be returned on call to doHandle", e);
    }
    return false;
  }
  @Override
  protected WRITEResponse doHandle(NFS4Handler server, Session session,
      WRITERequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }

    FileHandle fileHandle = session.getCurrentFileHandle();
    Path path = server.getPath(fileHandle);
    String file = path.toUri().getPath();
    FSDataOutputStream out = server.forWrite(request.getStateID(), session.getFileSystem(), fileHandle, false);

    LOGGER.info(session.getSessionID() + " xid = " + session.getXID() + ", write accepted " + file + " " + request.getOffset());

    WriteOrderHandler writeOrderHandler = server.getWriteOrderHandler(file, out);
    boolean sync = request.getStable() != NFS4_COMMIT_UNSTABLE4;
    int count = writeOrderHandler.write(path.toUri().getPath(), session.getXID(), request.getOffset(),
        sync, request.getData(), request.getStart(), request.getLength());

    WRITEResponse response = createResponse();
    OpaqueData8 verifer = new OpaqueData8();
    verifer.setData(Bytes.toBytes(server.getStartTime()));
    response.setVerifer(verifer);
    server.incrementMetric("HDFS_BYTES_WRITE", count);
    response.setCount(count);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected WRITEResponse createResponse() {
    return new WRITEResponse();
  }
}
