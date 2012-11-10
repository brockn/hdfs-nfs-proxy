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
package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.fest.reflect.core.Reflection.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.SortedSet;

import junit.framework.Assert;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.UserIDMapper;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCAcceptedException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCAuthException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.google.common.base.Charsets;

public class TestSecurityHandlerGSS {

  private static final int CONTEXT_ID = 1;
  private static final String DOMAIN = "LOCALDOMAIN";
  private static final String HOST = "localhost.localdomain";
  private static final String NFS_USER = "nfs";
  private static final int SEQ_NUM = 5000;
  private static final String SUPER_USER = UserIDMapper.getCurrentUser();
  private static final byte[] CLIENT_INIT_TOKEN = "clt".getBytes(Charsets.UTF_8);
  private static final byte[] SERVER_INIT_TOKEN = "srv".getBytes(Charsets.UTF_8);
  private static final byte[] GET_MIC_RESPONSE = "srv mic".getBytes(Charsets.UTF_8);
  private static final byte[] WRAP_RESPONSE = "some datas".getBytes(Charsets.UTF_8);

  private GSSManager gssManager;
  private GSSContext gssContext;
  private GSSName remoteUser;
  private SessionSecurityHandlerGSS securityHandler;
  private CredentialsGSS credentials;
  private RPCBuffer requestBuffer;
  
  @Before
  public void setup() throws Exception {
    gssManager = mock(GSSManager.class);
    gssContext = mock(GSSContext.class);
    credentials = new CredentialsGSS();
    remoteUser = mock(GSSName.class);
    when(gssManager.createContext(any(GSSCredential.class))).thenReturn(gssContext);
    when(gssContext.isEstablished()).thenReturn(true);
    when(gssContext.getSrcName()).thenReturn(remoteUser);
    when(remoteUser.toString()).thenReturn(String.format("%s/%s@%s", NFS_USER, HOST, DOMAIN));
    when(gssContext.acceptSecContext(any(byte[].class), anyInt(), anyInt())).
      thenReturn(SERVER_INIT_TOKEN);
    when(gssContext.getMIC(any(byte[].class), anyInt(), anyInt(), any(MessageProp.class)))
      .thenReturn(GET_MIC_RESPONSE);
    credentials.setVersion(RPCSEC_GSS_VERSION);
    credentials.setSequenceNum(SEQ_NUM);
    credentials.setService(RPCSEC_GSS_SERVICE_PRIVACY);
    credentials.setContext(Bytes.toBytes(CONTEXT_ID));
    securityHandler = new SessionSecurityHandlerGSS(credentials, gssManager, CONTEXT_ID, SUPER_USER);
    when(gssContext.unwrap(any(byte[].class), anyInt(), anyInt(), any(MessageProp.class))).
      thenReturn(Bytes.toBytes(SEQ_NUM));
    when(gssContext.wrap(any(byte[].class), anyInt(), anyInt(), any(MessageProp.class))).
      thenReturn(WRAP_RESPONSE);
    requestBuffer = new RPCBuffer();
    requestBuffer.writeUint32(CLIENT_INIT_TOKEN.length);
    requestBuffer.writeBytes(CLIENT_INIT_TOKEN);
    requestBuffer.flip();
  }
  @Test
  public void testWrapUnwarpRequired() throws Exception {
    Assert.assertTrue(securityHandler.isUnwrapRequired());
    Assert.assertTrue(securityHandler.isWrapRequired());
  }
  @Test
  public void testContextID() throws Exception {
    Assert.assertEquals(CONTEXT_ID, Bytes.toInt(securityHandler.getContextID()));
  }
  @Test
  public void testGetUserUnableToRetrievaName() throws Exception {
    when(gssContext.getSrcName()).thenThrow(new GSSException(0));
    try {
      securityHandler.getUser();
      Assert.fail();
    } catch (NFS4Exception e) {
      Assert.assertEquals(NFS4ERR_SERVERFAULT, e.getError());
    }
  }
  @Test
  public void testGetUserSuperUser() throws Exception {
    Assert.assertEquals(SUPER_USER, securityHandler.getUser());
  }
  @Test
  public void testGetUserAnonymous() throws Exception {
    when(gssContext.isEstablished()).thenReturn(false);
    Assert.assertEquals(ANONYMOUS_USERNAME, securityHandler.getUser());
  }
  @Test
  public void testGetUser() throws Exception {
    when(remoteUser.toString()).thenReturn(String.format("%s/%s@%s", "noland", HOST, DOMAIN));
    Assert.assertEquals("noland", securityHandler.getUser());
  }
  @Test
  public void testGetVeriferGetMICFails() throws Exception {
    RPCRequest request = new RPCRequest();
    request.setCredentials(credentials);
    when(gssContext.getMIC(any(byte[].class), anyInt(), anyInt(), any(MessageProp.class))).
      thenThrow(new GSSException(1, 2, "abc"));
    try {
      securityHandler.getVerifer(request);
      Assert.fail();
    } catch(RPCAuthException e) {
      Assert.assertEquals(RPC_AUTH_STATUS_GSS_CTXPROBLEM, e.getAuthState());
    }
  }
  @Test
  public void testGetVerifer() throws Exception {
    RPCRequest request = new RPCRequest();
    request.setCredentials(credentials);
    VerifierGSS verifier = securityHandler.getVerifer(request);
    Assert.assertTrue(Arrays.equals(GET_MIC_RESPONSE, verifier.get()));
  }
  @Test
  public void testInitializeContextGetMICFails() throws Exception {  
    credentials.setProcedure(RPCSEC_GSS_INIT);
    when(gssContext.getMIC(any(byte[].class), anyInt(), anyInt(), any(MessageProp.class))).
    thenThrow(new GSSException(1, 2, "abc"));
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierNone());
    request.setCredentials(credentials);
    try {
      securityHandler.initializeContext(request, requestBuffer);
      Assert.fail();
    } catch(RPCAuthException e) {
      Assert.assertEquals(RPC_AUTH_STATUS_GSS_CTXPROBLEM, e.getAuthState());
    }
  }
  @Test(expected = IllegalStateException.class)
  public void testInitializeContextBadProc() throws Exception {  
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierNone());
    request.setCredentials(credentials);
    securityHandler.initializeContext(request, requestBuffer);
  }
  @Test(expected = IllegalStateException.class)
  public void testInitializeContextBadVerifer() throws Exception {  
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierGSS());
    request.setCredentials(credentials);
    securityHandler.initializeContext(request, requestBuffer);
  }
  @Test
  public void testInitializeContextAcceptFails() throws Exception {
    credentials.setProcedure(RPCSEC_GSS_INIT);
    when(gssContext.isEstablished()).thenReturn(false);
    when(gssContext.acceptSecContext(any(byte[].class), anyInt(), anyInt())).
    thenThrow(new GSSException(1, 2, "abc"));
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierNone());
    request.setCredentials(credentials);
    Pair<? extends Verifier, InitializeResponse> result = 
        securityHandler.initializeContext(request, requestBuffer);
    Assert.assertEquals(VerifierNone.class, result.getFirst().getClass());
    InitializeResponse initResponse = result.getSecond();
    Assert.assertTrue(initResponse.getContextID().length == 0);
    Assert.assertEquals(1, initResponse.getMajorErrorCode());
    Assert.assertEquals(2, initResponse.getMinorErrorCode());
    Assert.assertEquals(RPCSEC_GSS_SEQUENCE_WINDOW, initResponse.getSequenceWindow());
    Assert.assertTrue(initResponse.getToken().length == 0);
  }
  @Test
  public void testInitializeContext() throws Exception {
    credentials.setProcedure(RPCSEC_GSS_INIT);
    when(gssContext.isEstablished()).thenReturn(false);
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierNone());
    request.setCredentials(credentials);
    Pair<? extends Verifier, InitializeResponse> result = 
        securityHandler.initializeContext(request, requestBuffer);
    Assert.assertTrue(Arrays.equals(GET_MIC_RESPONSE, ((VerifierGSS)result.getFirst()).get()));
    InitializeResponse initResponse = result.getSecond();
    Assert.assertEquals(CONTEXT_ID, Bytes.toInt(initResponse.getContextID()));
    Assert.assertEquals(RPCSEC_GSS_COMPLETE, initResponse.getMajorErrorCode());
    Assert.assertEquals(0, initResponse.getMinorErrorCode());
    Assert.assertEquals(RPCSEC_GSS_SEQUENCE_WINDOW, initResponse.getSequenceWindow());
    Assert.assertTrue(Arrays.equals(SERVER_INIT_TOKEN, initResponse.getToken()));
  }
  @Test
  public void testShouldSilentlyDropSeqToLarge() throws Exception {
    credentials.setSequenceNum(RPCSEC_GSS_MAX_SEQUENCE_NUMBER + 1);
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierNone());
    request.setCredentials(credentials);
    Assert.assertTrue(securityHandler.shouldSilentlyDrop(request));
  }
  @Test
  public void testShouldSilentlyDropAlreadSeen() throws Exception {
    @SuppressWarnings("unchecked")
    SortedSet<Integer> seqNums = field("mSequenceNumbers").
      ofType(SortedSet.class).in(securityHandler).get();
    seqNums.add(SEQ_NUM);
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierNone());
    request.setCredentials(credentials);
    Assert.assertTrue(securityHandler.shouldSilentlyDrop(request));
  }
  @Test
  public void testShouldSilentlyDropTooLow() throws Exception {
    @SuppressWarnings("unchecked")
    SortedSet<Integer> seqNums = field("mSequenceNumbers").
      ofType(SortedSet.class).in(securityHandler).get();
    seqNums.add(SEQ_NUM + 1);
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierNone());
    request.setCredentials(credentials);
    Assert.assertTrue(securityHandler.shouldSilentlyDrop(request));
  }
  @Test
  public void testShouldSilentlyDropCommonCase() throws Exception {
    @SuppressWarnings("unchecked")
    SortedSet<Integer> seqNums = field("mSequenceNumbers").
      ofType(SortedSet.class).in(securityHandler).get();
    seqNums.add(SEQ_NUM - 2);
    seqNums.add(SEQ_NUM - 1);
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierNone());
    request.setCredentials(credentials);
    Assert.assertFalse(securityHandler.shouldSilentlyDrop(request));
  }
  @Test
  public void testShouldSilentlyDropFirstRequests() throws Exception {
    RPCRequest request = new RPCRequest();
    request.setVerifier(new VerifierNone());
    request.setCredentials(credentials);
    Assert.assertFalse(securityHandler.shouldSilentlyDrop(request));
  }
  @Test(expected = IllegalStateException.class)
  public void testUnwrapBadProc() throws Exception {  
    RPCRequest request = new RPCRequest();
    request.setCredentials(credentials);
    credentials.setProcedure(-1);
    securityHandler.unwrap(request, new byte[0]);
  }
  @Test(expected = IllegalStateException.class)
  public void testUnwrapBadService() throws Exception {  
    RPCRequest request = new RPCRequest();
    request.setCredentials(credentials);
    credentials.setProcedure(-1);
    securityHandler.unwrap(request, new byte[0]);
  }
  @Test
  public void testUnwrapVerifyMICFails() throws Exception {
    RPCRequest request = new RPCRequest();
    VerifierGSS verifier = new VerifierGSS();
    verifier.set(new byte[0]);
    request.setVerifier(verifier);
    request.setCredentials(credentials);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new GSSException(1, 2, "abc");
      }      
    }).when(gssContext).verifyMIC(any(byte[].class), anyInt(), anyInt(), 
        any(byte[].class), anyInt(), anyInt(), any(MessageProp.class));
    try {
      securityHandler.unwrap(request, new byte[0]);
      Assert.fail();
    } catch(RPCAuthException e) {
      Assert.assertEquals(RPC_AUTH_STATUS_GSS_CREDPROBLEM, e.getAuthState());
    }
  }
  @Test
  public void testUnwrapUnwrapMICFails() throws Exception {
    RPCRequest request = new RPCRequest();
    VerifierGSS verifier = new VerifierGSS();
    verifier.set(new byte[0]);
    request.setVerifier(verifier);
    request.setCredentials(credentials);
    when(gssContext.unwrap(any(byte[].class), anyInt(), anyInt(), any(MessageProp.class))).
      thenThrow(new GSSException(1, 2, "abc"));
    try {
      securityHandler.unwrap(request, new byte[0]);
      Assert.fail();
    } catch(RPCAcceptedException e) {
      Assert.assertEquals(RPC_ACCEPT_GARBAGE_ARGS, e.getAcceptState());
    }
  }
  @Test
  public void testUnwrapSeqNumberNotEqual() throws Exception {
    RPCRequest request = new RPCRequest();
    VerifierGSS verifier = new VerifierGSS();
    verifier.set(new byte[0]);
    request.setVerifier(verifier);
    request.setCredentials(credentials);
    when(gssContext.unwrap(any(byte[].class), anyInt(), anyInt(), any(MessageProp.class))).
      thenReturn(Bytes.toBytes(SEQ_NUM + 1));
    try {
      securityHandler.unwrap(request, new byte[0]);
      Assert.fail();
    } catch(RPCAcceptedException e) {
      Assert.assertEquals(RPC_ACCEPT_GARBAGE_ARGS, e.getAcceptState());
    }
  }
  @Test
  public void testUnwrapExceedSequenceWindow() throws Exception {
    @SuppressWarnings("unchecked")
    SortedSet<Integer> seqNums = field("mSequenceNumbers").
      ofType(SortedSet.class).in(securityHandler).get();
    int firstSeqNumber = 1;
    for (int i = firstSeqNumber; i < RPCSEC_GSS_SEQUENCE_WINDOW + 1; i++) {
      seqNums.add(i);
    }
    RPCRequest request = new RPCRequest();
    VerifierGSS verifier = new VerifierGSS();
    verifier.set(new byte[0]);
    request.setVerifier(verifier);
    request.setCredentials(credentials);
    RPCBuffer buffer = securityHandler.unwrap(request, new byte[0]);
    Assert.assertEquals(buffer.position(), buffer.limit());
    Assert.assertFalse(seqNums.contains(firstSeqNumber));
  }
  @Test
  public void testUnwrapFirstRequest() throws Exception {
    RPCRequest request = new RPCRequest();
    VerifierGSS verifier = new VerifierGSS();
    verifier.set(new byte[0]);
    request.setVerifier(verifier);
    request.setCredentials(credentials);
    RPCBuffer buffer = securityHandler.unwrap(request, new byte[0]);
    Assert.assertEquals(buffer.position(), buffer.limit());
  }
  @Test
  public void testWrapWrapFails() throws Exception {
    when(gssContext.wrap(any(byte[].class), anyInt(), anyInt(), any(MessageProp.class))).
    thenThrow(new GSSException(1, 2, "abc"));
    MessageBase response = mock(MessageBase.class);
    RPCRequest request = new RPCRequest();
    request.setCredentials(credentials);
    try {
      securityHandler.wrap(request, response);
    } catch (RPCAcceptedException e) {
      Assert.assertEquals(RPC_ACCEPT_SYSTEM_ERR, e.getAcceptState());
    }
  }
  @Test
  public void testWrap() throws Exception {
    MessageBase response = mock(MessageBase.class);
    RPCRequest request = new RPCRequest();
    request.setCredentials(credentials);
    byte[] buffer = securityHandler.wrap(request, response);
    Assert.assertTrue(Arrays.equals(WRAP_RESPONSE, buffer));
  }
}
