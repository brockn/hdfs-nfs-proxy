/**
 * Copyright 2012 Cloudera Inc.
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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.OperationRequestHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OperationRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.PUTROOTFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OperationResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.PUTROOTFHResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.google.common.collect.Lists;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TestNFS4AsyncFuture {

  private OperationFactory operationFactory;
  private HDFSState hdfsState;
  private Session session;
  private UserGroupInformation ugi;
  private OperationRequestHandler<OperationRequest, OperationResponse> requestHandler;
  private PUTROOTFHRequest putRootFHRequest;
  private PUTROOTFHResponse putRootFHResponse;
  private NFS4AsyncFuture future;

  @Before
  public void setup() throws Exception {
    operationFactory = mock(OperationFactory.class);
    hdfsState = mock(HDFSState.class);
    session = mock(Session.class);
    ugi = mock(UserGroupInformation.class);
    requestHandler = mock(OperationRequestHandler.class);
    putRootFHRequest = new PUTROOTFHRequest();
    putRootFHResponse = new PUTROOTFHResponse();
    CompoundRequest compoundRequest = new CompoundRequest();
    List<OperationRequest> requests = Lists.newArrayList();
    requests.add(putRootFHRequest);
    compoundRequest.setOperations(requests);
    when(operationFactory.getHandler(anyInt())).thenReturn(requestHandler);
    when(session.getCompoundRequest()).thenReturn(compoundRequest);
    when(requestHandler.handle(hdfsState, session, putRootFHRequest)).thenReturn(putRootFHResponse);
    putRootFHResponse.setStatus(NFS4_OK);
    when(ugi.doAs(any(PrivilegedExceptionAction.class))).
    then(new Answer<AsyncFuture.Complete>() {
      @Override
      public AsyncFuture.Complete answer(InvocationOnMock invocation) throws Throwable {
        if((invocation.getArguments() != null) && (invocation.getArguments().length > 0)) {
          PrivilegedExceptionAction action =
              (PrivilegedExceptionAction) invocation.getArguments()[0];
          if(action != null) {
            return (AsyncFuture.Complete)(action).run();
          }
        }
        return null;
      }
    });
    future = new NFS4AsyncFuture(operationFactory, hdfsState, session, ugi);
  }
  @Test
  public void testFail() throws Exception {
    putRootFHResponse.setStatus(NFS4ERR_PERM);
    Assert.assertEquals(AsyncFuture.Complete.COMPLETE, future.makeProgress());
    Assert.assertEquals(NFS4ERR_PERM, future.get().getStatus());
  }
  @Test
  public void testSuccess() throws Exception {
    Assert.assertEquals(AsyncFuture.Complete.COMPLETE, future.makeProgress());
    Assert.assertEquals(NFS4_OK, future.get().getStatus());
    Assert.assertNotNull(future.toString());
  }
  @Test
  public void testWouldBlock() throws Exception {
    when(requestHandler.wouldBlock(any(HDFSState.class), any(Session.class),
        any(OperationRequest.class))).thenReturn(true);
    Assert.assertEquals(AsyncFuture.Complete.RETRY, future.makeProgress());
  }
  @Test
  public void testThrowsIOException() throws Exception {
    when(ugi.doAs(any(PrivilegedExceptionAction.class))).
      then(new Answer<AsyncFuture.Complete>() {
        @Override
        public AsyncFuture.Complete answer(InvocationOnMock invocation) throws Throwable {
          throw new IOException();
        }
      });
    Assert.assertEquals(AsyncFuture.Complete.COMPLETE, future.makeProgress());
    Assert.assertEquals(NFS4ERR_SERVERFAULT, future.get().getStatus());
  }
  @Test
  public void testThrowsNextedIOException() throws Exception {
    when(ugi.doAs(any(PrivilegedExceptionAction.class))).
      then(new Answer<AsyncFuture.Complete>() {
        @Override
        public AsyncFuture.Complete answer(InvocationOnMock invocation) throws Throwable {
          throw new IOException(new IOException());
        }
      });
    Assert.assertEquals(AsyncFuture.Complete.COMPLETE, future.makeProgress());
    Assert.assertEquals(NFS4ERR_SERVERFAULT, future.get().getStatus());
  }

  @Test
  public void testThrowsNFS4Exception() throws Exception {
    when(ugi.doAs(any(PrivilegedExceptionAction.class))).
      then(new Answer<AsyncFuture.Complete>() {
        @Override
        public AsyncFuture.Complete answer(InvocationOnMock invocation) throws Throwable {
          throw new NFS4Exception(NFS4ERR_BAD_COOKIE);
        }
      });
    Assert.assertEquals(AsyncFuture.Complete.COMPLETE, future.makeProgress());
    Assert.assertEquals(NFS4ERR_BAD_COOKIE, future.get().getStatus());
  }
  @Test
  public void testThrowsNestedNFS4Exception() throws Exception {
    when(ugi.doAs(any(PrivilegedExceptionAction.class))).
      then(new Answer<AsyncFuture.Complete>() {
        @Override
        public AsyncFuture.Complete answer(InvocationOnMock invocation) throws Throwable {
          throw new IOException(new NFS4Exception(NFS4ERR_BAD_COOKIE));
        }
      });
    Assert.assertEquals(AsyncFuture.Complete.COMPLETE, future.makeProgress());
    Assert.assertEquals(NFS4ERR_BAD_COOKIE, future.get().getStatus());
  }
  @Test
  public void testThrowsUnsupportedOperationException() throws Exception {
    when(ugi.doAs(any(PrivilegedExceptionAction.class))).
      then(new Answer<AsyncFuture.Complete>() {
        @Override
        public AsyncFuture.Complete answer(InvocationOnMock invocation) throws Throwable {
          throw new UnsupportedOperationException();
        }
      });
    Assert.assertEquals(AsyncFuture.Complete.COMPLETE, future.makeProgress());
    Assert.assertEquals(NFS4ERR_NOTSUPP, future.get().getStatus());
  }
}
