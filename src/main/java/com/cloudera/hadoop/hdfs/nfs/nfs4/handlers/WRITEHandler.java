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

import static com.cloudera.hadoop.hdfs.nfs.metrics.MetricConstants.Metric.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileBackedWrite;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MemoryBackedWrite;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.PendingWrite;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.WRITERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.WRITEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSOutputStream;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class WRITEHandler extends OperationRequestHandler<WRITERequest, WRITEResponse> {

  protected static final Logger LOGGER = Logger.getLogger(WRITEHandler.class);
  @Override
  protected WRITEResponse createResponse() {
    return new WRITEResponse();
  }
  @Override
  protected WRITEResponse doHandle(HDFSState hdfsState, Session session,
      WRITERequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }

    FileHandle fileHandle = session.getCurrentFileHandle();
    Path path = hdfsState.getPath(fileHandle);
    String file = path.toUri().getPath();
    HDFSOutputStream out = hdfsState.forWrite(request.getStateID(), fileHandle);

    LOGGER.info(session.getSessionID() + " xid = " + session.getXIDAsHexString() + 
        ", write accepted " + file + " " + request.getOffset());

    WriteOrderHandler writeOrderHandler = hdfsState.getOrCreateWriteOrderHandler(session.getCurrentFileHandle());
    boolean sync = request.getStable() != NFS4_COMMIT_UNSTABLE4;
    boolean wouldBlock = writeOrderHandler.writeWouldBlock(request.getOffset());
    if(sync  && wouldBlock) {
      /* 
       * See comment above. In this case, we cannot honor the sync request
       */
      sync = false;
      LOGGER.info("Attempted synchronous random write " + file + ", offset = " + request.getOffset());
    }
    PendingWrite write = null;
    if(request.getOffset() > out.getPos() + ONE_MB) {
      File backingFile = writeOrderHandler.getTemporaryFile(String.valueOf(request.getOffset()));
      write = new FileBackedWrite(backingFile, path.toUri().getPath(), session.getXID(), request.getOffset(),
          sync, request.getData(), request.getStart(), request.getLength());
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("Created file backed write at " + write);
      }
    } else {
      write = new MemoryBackedWrite(path.toUri().getPath(), session.getXID(), request.getOffset(),
          sync, request.getData(), request.getStart(), request.getLength());
    }
    int count = writeOrderHandler.write(write);
    WRITEResponse response = createResponse();
    OpaqueData8 verifer = new OpaqueData8();
    verifer.setData(Bytes.toBytes(hdfsState.getStartTime()));
    response.setVerifer(verifer);
    hdfsState.incrementMetric(HDFS_BYTES_WRITE, count);
    response.setCount(count);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  public boolean wouldBlock(HDFSState hdfsState, Session session, WRITERequest request) {
    /*
     * I have observed NFS clients which will send some writes with
     * a sync flag and some other without a sync flag. I believe this
     * is when feels the nfs server is responding slow or when
     * the nfs server sends errors. I know for a fact it will 
     * will do this during the error condition to try and bubble
     * the error back up to the writer.
     * 
     * However, in the case where we have not received the pre-req writes
     * we cannot honor the sync flag. This should only occur when the
     * kernel adds the sync flag to write, not when someone calls fsync
     * on the stream.
     * 
     * We could honor this by queuing this write but it's observed that 
     * clients (ubuntu 12.10) will stop sending write requests waiting
     * for the response to the previous requests.
     */
    return false;
  }
}
