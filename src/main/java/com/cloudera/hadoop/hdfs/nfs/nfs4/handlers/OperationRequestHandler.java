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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OperationRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OperationResponse;

/**
 * Subclasses process a specific Request, Response pair. They MUST
 * be stateless as one instance will be created per JVM.
 * @param <IN>
 * @param <OUT>
 */
public abstract class OperationRequestHandler<IN extends OperationRequest, OUT extends OperationResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(OperationRequestHandler.class);

  /**
   * Handle request and any exception throwing during the process.
   * @param server
   * @param session
   * @param request
   * @return response of correct type regardless of an
   * exception being thrown during implementing classes
   * handling of request.
   */
  public OUT handle(NFS4Handler server, Session session, IN request) {
    try {
      return doHandle(server, session, request);
    } catch(Exception ex) {
      server.incrementMetric("EXCEPTION_" + ex.getClass().getSimpleName(), 1);
      OUT response = createResponse();
      boolean log = true;
      if(ex instanceof NFS4Exception) {
        NFS4Exception nfsEx = (NFS4Exception)ex;
        log = !nfsEx.shouldLog();
        response.setStatus(nfsEx.getError());
      } else if(ex instanceof FileNotFoundException) {
        response.setStatus(NFS4ERR_NOENT);
      } else if(ex instanceof IOException) {
        response.setStatus(NFS4ERR_IO);
      } else if(ex instanceof IllegalArgumentException) {
        response.setStatus(NFS4ERR_INVAL);
      } else if(ex instanceof UnsupportedOperationException) {
        response.setStatus(NFS4ERR_NOTSUPP);
      } else if(ex instanceof AccessControlException) {
        response.setStatus(NFS4ERR_PERM);
      } else {
        response.setStatus(NFS4ERR_SERVERFAULT);
      }
      String msg = session.getSessionID() + " Error for client " + 
          session.getClientHostPort() + " and " + response.getClass().getSimpleName();
      if(log || LOGGER.isDebugEnabled()) {
        LOGGER.warn(msg, ex);
      } else {
        LOGGER.warn(msg);
      }
      return response;
    }
  }
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
  protected abstract OUT doHandle(NFS4Handler server, Session session, IN request) 
      throws NFS4Exception, IOException, UnsupportedOperationException;  
  
  /**
   * @return a response object of the correct type. Used so the handle() method
   * can return an object of the correct type when an error is encountered.
   */
  protected abstract OUT createResponse();
}
