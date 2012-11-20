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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.net.InetAddress;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.ietf.jgss.GSSManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCAuthException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCResponse;
import com.google.common.base.Supplier;

public class TestSecurityHandlerFactory {

  private static final int CONTEXT_ID = 1;
  private static final int SEQ_NUM = 5000;
  private Configuration conf;
  private Supplier<GSSManager> gssManagerSupplier;
  private GSSManager gssManager;
  private SecurityHandlerFactory securityHandlerFactory;
  private CredentialsGSS credentialsGSS;
  private CredentialsSystem credentialsSystem;
  private String sessionID;
  private String clientName;
  private RPCRequest request;
  private RPCBuffer requestBuffer;
  private VerifierGSS veriferGSS;
  private InitializeResponse initResponse;
  private SessionSecurityHandlerGSSFactory sessionSecurityHandlerGSSFactory;

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    conf.set(SECURITY_ALLOWED_HOSTS, "192.168.0.0/22 rw");
    conf.set(SECURITY_FLAVOR, SECURITY_FLAVOR_KERBEROS);
    byte[] contextID = Bytes.toBytes(CONTEXT_ID);
    sessionID = "sessionID";
    clientName = "client";
    credentialsGSS = new CredentialsGSS();
    credentialsGSS.setVersion(RPCSEC_GSS_VERSION);
    credentialsGSS.setSequenceNum(SEQ_NUM);
    credentialsGSS.setService(RPCSEC_GSS_SERVICE_PRIVACY);
    credentialsGSS.setContext(contextID);
    credentialsGSS.setProcedure(RPCSEC_GSS_DATA);
    credentialsSystem = new CredentialsSystem();
    request = new RPCRequest();
    requestBuffer = new RPCBuffer();
    request.setCredentials(credentialsGSS);
    gssManager = mock(GSSManager.class);
    gssManagerSupplier = new Supplier<GSSManager>() {
      @Override
      public GSSManager get() {
        return gssManager;
      }
    };
    initResponse = new InitializeResponse();
    initResponse.setContextID(contextID);
    veriferGSS = new VerifierGSS();
    veriferGSS.set(contextID);
    SessionSecurityHandlerGSS securityHandler = mock(SessionSecurityHandlerGSS.class);
    when(securityHandler.initializeContext(request, requestBuffer)).then(
        new Answer<Pair<? extends Verifier, InitializeResponse> >() {
      @Override
      public Pair<? extends Verifier, InitializeResponse>  answer(InvocationOnMock invocation)
          throws Throwable {
        return new Pair<VerifierGSS, InitializeResponse>(veriferGSS, initResponse);
      }
    });
    when(securityHandler.getContextID()).thenReturn(credentialsGSS.getContext().getData());
    sessionSecurityHandlerGSSFactory = mock(SessionSecurityHandlerGSSFactory.class);
    when(sessionSecurityHandlerGSSFactory.
        getInstance(any(CredentialsGSS.class), any(GSSManager.class), anyInt(), anyString())).
        thenReturn(securityHandler);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyAllowedHosts() throws Exception {
    conf.set(SECURITY_ALLOWED_HOSTS, "");
    new SecurityHandlerFactory(conf, gssManagerSupplier, sessionSecurityHandlerGSSFactory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadSecurityFlavor() throws Exception {
    conf.set(SECURITY_FLAVOR, "");
    new SecurityHandlerFactory(conf, gssManagerSupplier, sessionSecurityHandlerGSSFactory);
  }
  @Test
  public void testClientHostsTrue() throws Exception {
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    InetAddress addr = mock(InetAddress.class);
    when(addr.getHostAddress()).thenReturn("192.168.0.1");
    Assert.assertEquals(AccessPrivilege.READ_WRITE, securityHandlerFactory.getAccessPrivilege(addr));
  }
  @Test
  public void testClientHostsFalse()throws Exception {
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    InetAddress addr = mock(InetAddress.class);
    when(addr.getHostAddress()).thenReturn("10.0.0.1");
    Assert.assertEquals(AccessPrivilege.NONE, securityHandlerFactory.getAccessPrivilege(addr));
  }
  @Test
  public void testSystemCredsFailWithKerberosEnabled() throws Exception {
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    try {
      securityHandlerFactory.getSecurityHandler(credentialsSystem);
    } catch (RPCAuthException e) {
      Assert.assertEquals(RPC_AUTH_STATUS_BADCRED, e.getAuthState());
    }
  }
  @Test
  public void testKerberosCredsFailWithSystemEnabled() throws Exception {
    conf.set(SECURITY_FLAVOR, SECURITY_FLAVOR_DEFAULT);
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    try {
      securityHandlerFactory.getSecurityHandler(credentialsGSS);
      Assert.fail();
    } catch (RPCAuthException e) {
      Assert.assertEquals(RPC_AUTH_STATUS_REJECTEDCRED, e.getAuthState());
    }
  }
  @Test
  public void testContextIDDoesNotExist() throws Exception {
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    try {
      securityHandlerFactory.getSecurityHandler(credentialsGSS);
      Assert.fail();
    } catch (RPCAuthException e) {
      Assert.assertEquals(RPC_AUTH_STATUS_GSS_CREDPROBLEM, e.getAuthState());
    }
  }
  @Test
  public void testSecurityDisabledNullRequestForKerberos() throws Exception {
    conf.set(SECURITY_FLAVOR, SECURITY_FLAVOR_DEFAULT);
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    try {
      securityHandlerFactory.handleNullRequest(sessionID, clientName, request, requestBuffer);
      Assert.fail();
    } catch (RPCAuthException e) {
      Assert.assertEquals(RPC_AUTH_STATUS_BADCRED, e.getAuthState());
    }
  }
  @Test
  public void testSecurityEnabledNullRequestForSystem() throws Exception {
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    request.setCredentials(credentialsSystem);
    securityHandlerFactory.handleNullRequest(sessionID, clientName, request, requestBuffer);
  }
  @Test
  public void testSecurityEnabledNullRequestBadProc() throws Exception {
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    try {
      securityHandlerFactory.handleNullRequest(sessionID, clientName, request, requestBuffer);
      Assert.fail();
    } catch (RPCAuthException e) {
      Assert.assertEquals(RPC_AUTH_STATUS_BADCRED, e.getAuthState());
    }
  }
  @Test
  public void testSecurityEnabledNullRequestInit() throws Exception {
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    credentialsGSS.setProcedure(RPCSEC_GSS_INIT);
    RPCBuffer responseBuffer = securityHandlerFactory.
        handleNullRequest(sessionID, clientName, request, requestBuffer);
    Assert.assertEquals(Integer.MAX_VALUE, responseBuffer.readInt()); // length
    RPCResponse response = new RPCResponse();
    response.read(responseBuffer);
    Assert.assertEquals(RPC_REPLY_STATE_ACCEPT, response.getReplyState());
    Assert.assertEquals(RPC_ACCEPT_SUCCESS, response.getAcceptState());
  }
  @Test
  public void testSecurityEnabledNullRequestContinueInit() throws Exception {
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    credentialsGSS.setProcedure(RPCSEC_GSS_CONTINUE_INIT);
    RPCBuffer responseBuffer = securityHandlerFactory.
        handleNullRequest(sessionID, clientName, request, requestBuffer);
    Assert.assertEquals(Integer.MAX_VALUE, responseBuffer.readInt()); // length
    RPCResponse response = new RPCResponse();
    response.read(responseBuffer);
    Assert.assertEquals(RPC_REPLY_STATE_ACCEPT, response.getReplyState());
    Assert.assertEquals(RPC_ACCEPT_SUCCESS, response.getAcceptState());
  }
  @Test
  public void testSecurityEnabledNullRequestDestory() throws Exception {
    securityHandlerFactory = new SecurityHandlerFactory(conf, gssManagerSupplier,
        sessionSecurityHandlerGSSFactory);
    credentialsGSS.setProcedure(RPCSEC_GSS_DESTROY);
    RPCBuffer responseBuffer = securityHandlerFactory.
        handleNullRequest(sessionID, clientName, request, requestBuffer);
    Assert.assertEquals(Integer.MAX_VALUE, responseBuffer.readInt()); // length
    RPCResponse response = new RPCResponse();
    response.read(responseBuffer);
    Assert.assertEquals(RPC_REPLY_STATE_ACCEPT, response.getReplyState());
    Assert.assertEquals(RPC_ACCEPT_SUCCESS, response.getAcceptState());
    Assert.assertEquals(VerifierNone.class, response.getVerifier().getClass());
  }
}