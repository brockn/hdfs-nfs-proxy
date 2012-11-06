/**
 * Copyright 2012 The Apache Software Foundation
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

import junit.framework.Assert;

import org.apache.hadoop.security.AccessControlException;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SAVEFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SAVEFHResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class TestOperationRequestHandler extends TestBaseHandler {

  @Test
  public void testAccessControlException() throws Exception {
    OperationRequestHandler<SAVEFHRequest, SAVEFHResponse> handler = 
        new OperationRequestHandler<SAVEFHRequest, SAVEFHResponse>() {
      @Override
      protected SAVEFHResponse createResponse() {
        return new SAVEFHResponse();
      }
      @Override
      protected SAVEFHResponse doHandle(HDFSState hdfsState,
          Session session, SAVEFHRequest request) throws NFS4Exception,
          IOException, UnsupportedOperationException {
        throw new AccessControlException();
      }      
    };
    SAVEFHResponse response = handler.handle(hdfsState, session, null);
    Assert.assertEquals(NFS4ERR_PERM, response.getStatus());
  }
  @Test
  public void testIllegalArgumentException() throws Exception {
    OperationRequestHandler<SAVEFHRequest, SAVEFHResponse> handler = 
        new OperationRequestHandler<SAVEFHRequest, SAVEFHResponse>() {
      @Override
      protected SAVEFHResponse createResponse() {
        return new SAVEFHResponse();
      }
      @Override
      protected SAVEFHResponse doHandle(HDFSState hdfsState,
          Session session, SAVEFHRequest request) throws NFS4Exception,
          IOException, UnsupportedOperationException {
        throw new IllegalArgumentException();
      }      
    };
    SAVEFHResponse response = handler.handle(hdfsState, session, null);
    Assert.assertEquals(NFS4ERR_INVAL, response.getStatus());
  }
  @Test
  public void testRuntimeException() throws Exception {
    OperationRequestHandler<SAVEFHRequest, SAVEFHResponse> handler = 
        new OperationRequestHandler<SAVEFHRequest, SAVEFHResponse>() {
      @Override
      protected SAVEFHResponse createResponse() {
        return new SAVEFHResponse();
      }
      @Override
      protected SAVEFHResponse doHandle(HDFSState hdfsState,
          Session session, SAVEFHRequest request) throws NFS4Exception,
          IOException, UnsupportedOperationException {
        throw new RuntimeException();
      }      
    };
    SAVEFHResponse response = handler.handle(hdfsState, session, null);
    Assert.assertEquals(NFS4ERR_SERVERFAULT, response.getStatus());
  }
  @Test
  public void testUnsupportedOperationException() throws Exception {
    OperationRequestHandler<SAVEFHRequest, SAVEFHResponse> handler = 
        new OperationRequestHandler<SAVEFHRequest, SAVEFHResponse>() {
      @Override
      protected SAVEFHResponse createResponse() {
        return new SAVEFHResponse();
      }
      @Override
      protected SAVEFHResponse doHandle(HDFSState hdfsState,
          Session session, SAVEFHRequest request) throws NFS4Exception,
          IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
      }      
    };
    SAVEFHResponse response = handler.handle(hdfsState, session, null);
    Assert.assertEquals(NFS4ERR_NOTSUPP, response.getStatus());
  }
}
