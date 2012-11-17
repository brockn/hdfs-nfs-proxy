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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.security.AccessControlException;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OperationRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OperationResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.cloudera.hadoop.hdfs.nfs.security.AccessPrivilege;

/**
 * Subclasses process a specific Request, Response pair. They MUST be stateless
 * as one instance will be created per JVM.
 *
 * @param <IN>
 * @param <OUT>
 */
public abstract class OperationRequestHandler<IN extends OperationRequest, OUT extends OperationResponse> {

  protected static final Logger LOGGER = Logger.getLogger(OperationRequestHandler.class);

  /**
   * @return a response object of the correct type. Used so the handle()
   * method can return an object of the correct type when an error is
   * encountered.
   */
  protected abstract OUT createResponse();
  /**
   * Implementing classes actually handle the request in this method.
   *
   * @param server
   * @param session
   * @param request
   * @return
   * @throws NFS4Exception
   * @throws IOException
   * @throws UnsupportedOperationException
   */
  protected abstract OUT doHandle(HDFSState hdfsState, Session session, IN request)
      throws NFS4Exception, IOException, UnsupportedOperationException;

  /**
   * Handle request and any exception throwing during the process.
   *
   * @param server
   * @param session
   * @param request
   * @return response of correct type regardless of an exception being thrown
   * during implementing classes handling of request.
   */
  public OUT handle(HDFSState hdfsState, Session session, IN request) {
    try {
      if(isWriteOnlyHandler() && AccessPrivilege.READ_WRITE != session.getAccessPrivilege()) {
        throw new NFS4Exception(NFS4ERR_PERM);
      }
      return doHandle(hdfsState, session, request);
    } catch (Exception ex) {
      hdfsState.incrementMetric("EXCEPTION_" + ex.getClass().getSimpleName(), 1);
      OUT response = createResponse();
      if (ex instanceof NFS4Exception) {
        NFS4Exception nfsEx = (NFS4Exception) ex;
        response.setStatus(nfsEx.getError());
      } else if (ex instanceof FileNotFoundException) {
        response.setStatus(NFS4ERR_NOENT);
      } else if (ex instanceof AccessControlException) {
        response.setStatus(NFS4ERR_PERM);
      } else if (ex instanceof IOException) {
        response.setStatus(NFS4ERR_IO);
      } else if (ex instanceof IllegalArgumentException) {
        response.setStatus(NFS4ERR_INVAL);
      } else if (ex instanceof UnsupportedOperationException) {
        response.setStatus(NFS4ERR_NOTSUPP);
      } else {
        response.setStatus(NFS4ERR_SERVERFAULT);
      }
      String msg = session.getSessionID() + " Error for client "
          + session.getClientAddress() + " and " + response.getClass().getSimpleName();
      LOGGER.warn(msg, ex);
      return response;
    }
  }

  boolean isWriteOnlyHandler() {
    return false;
  }
  public boolean wouldBlock(HDFSState server, Session session, IN request) {
    return false;
  }
}
