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

package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.LOCALHOST;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_DATA;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_INIT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_SERVICE_NONE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_SERVICE_PRIVACY;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPCSEC_GSS_VERSION;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_ACCEPT_SUCCESS;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_REPLY_STATE_ACCEPT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_VERSION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.MessageProp;
import org.ietf.jgss.Oid;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.nfs4.BaseClient;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCTestUtil;


// SUPER UGLY HACK CLASS WHICH WILL BE REMOVED
// I USED IT FOR LEARNING ABOUT JAVA GSS-API

public class SecureClient extends BaseClient {

  protected Socket mClient;
  protected InputStream mInputStream;
  protected OutputStream mOutputStream;
  protected OpaqueData mContextID;
  protected int mSequenceNumber = 0;
  protected int mSequenceWindow;
  protected GSSContext mContext;

  public SecureClient() throws Exception {

    GSSManager gssManager = GSSManager.getInstance();

    mClient = new Socket(LOCALHOST, 2049);

    mInputStream = mClient.getInputStream();
    mOutputStream = mClient.getOutputStream();

    Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");
    GSSName serverName = gssManager.createName(
        "noland@LOCALDOMAIN", null);
    mContext = gssManager.createContext(serverName, krb5Oid, null,
        GSSContext.DEFAULT_LIFETIME);

    mContext.requestMutualAuth(true);
    mContext.requestConf(true);
    mContext.requestInteg(true);

    byte[] token = new byte[0];

    RPCResponse response = null;
    int requestSeqNumber  = -1;

    while (!mContext.isEstablished()) {
      token = mContext.initSecContext(token, 0, token.length);
      if (token != null) {

        requestSeqNumber = mSequenceNumber++;

        RPCRequest request = new RPCRequest(1, RPC_VERSION);
        CredentialsGSS creds = new CredentialsGSS();
        creds.setProcedure(RPCSEC_GSS_INIT);
        creds.setSequenceNum(requestSeqNumber);
        creds.setVersion(RPCSEC_GSS_VERSION);
        creds.setService(RPCSEC_GSS_SERVICE_NONE);
        creds.setContext(token);
        request.setCredentials(creds);
        request.setVerifier(new VerifierNone());

        RPCBuffer requestBuffer = new RPCBuffer();
        request.write(requestBuffer);
        System.out.println("Writing token " + token.length + ": "
            + Bytes.asHex(token));
        requestBuffer.writeUint32(token.length);
        requestBuffer.writeBytes(token);
        requestBuffer.write(mOutputStream);

        RPCBuffer responseBuffer = RPCBuffer.from(mInputStream);
        response = new RPCResponse();
        response.read(responseBuffer);

        mContextID = new OpaqueData(responseBuffer.readBytes());

        int major = responseBuffer.readUint32();
        System.out.println("major = " + major);
        int minor = responseBuffer.readUint32();
        System.out.println("minor = " + minor);
        mSequenceWindow = responseBuffer.readUint32();
        System.out.println("sequenceWindow = " + mSequenceWindow);
        token = responseBuffer.readBytes();

      }
    }

    System.out.println("established = " + mContext.isEstablished());
    System.out.println("mutual auth = " + mContext.getMutualAuthState());

    checkNotNull(response);
    checkArgument(requestSeqNumber >= 0);

    verify(requestSeqNumber, response);

    initialize();
  }

  protected void verify(int sequenceNumber, RPCResponse response) {
    byte[] verifer = ((VerifierGSS) response.getVerifier()).get();
    byte[] seqNumberBytes = Bytes.toBytes(sequenceNumber);
    try {
      mContext.verifyMIC(verifer, 0, verifer.length, seqNumberBytes, 0,
          seqNumberBytes.length, new MessageProp(false));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  protected byte[] createVerifer(RPCRequest request) {
    RPCBuffer buffer = new RPCBuffer();
    request.write(buffer);
    buffer.flip();

    int length = buffer.length();
    LOGGER.info("Reading " + length);
    byte[] data = buffer.readBytes(length);

    try {
      // skip the first 4 bytes which are the length
      return mContext.getMIC(data, 3, data.length - 4, new MessageProp(false));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  public byte[] wrap(MessageBase response) {
    RPCBuffer buffer = new RPCBuffer();
    response.write(buffer);
    buffer.flip();
    int length = buffer.length();
    LOGGER.info("Reading " + length);
    byte[] data = buffer.readBytes(length);
    try {
      return mContext.wrap(data, 0, data.length, new MessageProp(true));
    } catch(GSSException ex) {
      LOGGER.warn("Error in getMIC", ex);
      throw new RuntimeException(ex);
    }
  }


  public byte[] unwrap(byte[] data) {
    try {
      return mContext.unwrap(data, 0, data.length, new MessageProp(true));
    } catch(GSSException ex) {
      LOGGER.warn("Error in getMIC", ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected CompoundResponse doMakeRequest(CompoundRequest request)
      throws IOException {

    int sequenceNumber = ++mSequenceNumber;
    RPCBuffer buffer = new RPCBuffer();
    RPCRequest rpcRequest = RPCTestUtil.createRequest();

    CredentialsGSS creds = new CredentialsGSS();
    creds.setProcedure(RPCSEC_GSS_DATA);
    creds.setSequenceNum(sequenceNumber);
    creds.setVersion(RPCSEC_GSS_VERSION);
    creds.setService(RPCSEC_GSS_SERVICE_PRIVACY);
    creds.setContext(mContextID);
    rpcRequest.setCredentials(creds);

    VerifierGSS verifier = new VerifierGSS();
    verifier.set(createVerifer(rpcRequest));

    rpcRequest.setVerifier(verifier);

    rpcRequest.write(buffer);

    byte[] data = wrap(request);

    buffer.writeUint32(data.length);
    buffer.writeBytes(data);


    buffer.flip();


    buffer.write(mOutputStream);
    mOutputStream.flush();

    buffer = RPCBuffer.from(mInputStream);
    RPCResponse rpcResponse = new RPCResponse();
    rpcResponse.read(buffer);

    verify(sequenceNumber, rpcResponse);


    assertEquals(rpcRequest.getXid(), rpcResponse.getXid());
    assertEquals(RPC_REPLY_STATE_ACCEPT, rpcResponse.getReplyState());
    assertEquals(RPC_ACCEPT_SUCCESS, rpcResponse.getAcceptState());

    try {
      byte[] encryptedData = buffer.readBytes();
      byte[] plainData = unwrap(encryptedData);
      buffer = new RPCBuffer(plainData);
      CompoundResponse response = new CompoundResponse();
      response.read(buffer);
      return response;

    } catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void shutdown() {
    try {
      mClient.close();
    } catch (Exception ex) {
    }
  }




  public static void main(String[] args) throws Exception {

    System.setProperty("java.security.krb5.realm", "LOCALDOMAIN");
    System.setProperty("java.security.krb5.kdc", "localhost");
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    System.setProperty("java.security.auth.login.config", "sec.conf");


    SecureClient client = new SecureClient();
    client.shutdown();
  }
}
