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
package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.metrics.MetricConstants.Metric.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RequiresCredentials;
import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsGSS;
import com.cloudera.hadoop.hdfs.nfs.security.SecurityHandler;
import com.cloudera.hadoop.hdfs.nfs.security.Verifier;
import com.cloudera.hadoop.hdfs.nfs.security.VerifierNone;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * ClientWorker handles a socket connection to the server and terminates when
 * the input stream throws an EOF Exception. As all requests execute in an async
 * fashion, the worker sleeps under two conditions: 1) the client retransmits
 * requests past a certain threshold 2) the client submits a requests and the
 * number of in progress requests exceeds some threshold. This sleep obviously
 * stops the client from sending addition requests and likely causes the
 * underlying TCP client to be sent the TCP SLOW DOWN packet.
 *
 * @param <REQUEST>
 * @param <RESPONSE>
 */
class ClientInputHandler<REQUEST extends MessageBase, RESPONSE extends MessageBase> extends Thread {

  protected static final Logger LOGGER = Logger.getLogger(ClientInputHandler.class);
  protected static final long RETRANSMIT_PENALTY_TIME = 100L;
  protected static final long TOO_MANY_PENDING_REQUESTS_PAUSE = 100L;
  protected static final long EXECUTOR_THREAD_POOL_FULL_PAUSE = 10L;
  protected int mRestransmitPenaltyThreshold;
  protected int mMaxPendingRequests;
  protected int mMaxPendingRequestWaits;
  protected Socket mClient;
  protected String mClientName;
  protected RPCServer<REQUEST, RESPONSE> mRPCServer;
  protected ClientOutputHandler mOutputHandler;
  protected BlockingQueue<RPCBuffer> mOutputBufferQueue;
  protected RPCHandler<REQUEST, RESPONSE> mHandler;
  protected SecurityHandler mSecurityHandler;
  protected ConcurrentMap<Integer, Long> mRequestsInProgress;
  protected Map<Integer, MessageBase> mResponseCache;
  /**
   * Number of retransmits received
   */
  protected long mRetransmits = 0L;
  protected String mSessionID;
  protected Configuration mConfiguration;

  protected static final AtomicInteger SESSIONID = new AtomicInteger(Integer.MAX_VALUE);

  public ClientInputHandler(Configuration conf, RPCServer<REQUEST, RESPONSE> server, RPCHandler<REQUEST, RESPONSE> handler, Socket client) {
    mConfiguration = conf;
    mRPCServer = server;
    mHandler = handler;
    mClient = client;
    mSecurityHandler = SecurityHandler.getInstance(mConfiguration);
    LOGGER.info("Got SecurityHandler of type " + mSecurityHandler.getClass().getName());
    String clientHost = mClient.getInetAddress().getCanonicalHostName();
    mClientName = clientHost + ":" + mClient.getPort();
    mSessionID = "0x" + Integer.toHexString(SESSIONID.addAndGet(-5));
    setName("RPCServer-" + mClientName);
    mOutputBufferQueue = mRPCServer.getOutputQueue(clientHost);

    mMaxPendingRequests = mConfiguration.getInt(RPC_MAX_PENDING_REQUESTS, 20);
    mRestransmitPenaltyThreshold = mConfiguration.getInt(RPC_RETRANSMIT_PENALTY_THRESHOLD, 3);
    mMaxPendingRequestWaits = 500;
    
    mRequestsInProgress = mRPCServer.getRequestsInProgress();
    mResponseCache = mRPCServer.getResponseCache();
  }

  @Override
  public void run() {
    InputStream in = null;
    OutputStream out = null;
    RPCRequest request = null;
    try {
      mClient.setTcpNoDelay(true);
      mClient.setPerformancePreferences(0, 1, 0);
      in = mClient.getInputStream();
      out = mClient.getOutputStream();
      mOutputHandler = new ClientOutputHandler(out, mOutputBufferQueue, mClientName);
      mOutputHandler.setDaemon(true);
      mOutputHandler.start();
      while(true) {
        // request is used to indicate if we should send
        // a failure packet in case of an error
        request = null;
        mRetransmits = mRetransmits > 0 ? mRetransmits : 0;
        
        RPCBuffer requestBuffer = RPCBuffer.from(in);
        LOGGER.info(mSessionID + " got request");
        mHandler.incrementMetric(CLIENT_BYTES_READ, requestBuffer.length());
        request = new RPCRequest();
        request.read(requestBuffer);
        if(request.getRpcVersion() != RPC_VERSION) {
          LOGGER.info(mSessionID + " Denying client due to bad RPC version " + request.getRpcVersion() + " for " + mClientName);
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_REJECT_MISMATCH);
          response.setVerifier(new VerifierNone());
          writeRPCResponse(response);
        } else if(request.getCredentials() == null){
          LOGGER.info(mSessionID + " Denying client due to null credentials for " + mClientName);
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_REJECT_AUTH_ERROR);
          response.setAuthState(RPC_AUTH_STATUS_TOOWEAK);
          response.setVerifier(new VerifierNone());
          writeRPCResponse(response);
        } else if(request.getProcedure() == NFS_PROC_NULL) {
          // RPCGSS uses the NULL proc to setup
          if(request.getCredentials() instanceof CredentialsGSS) {
            Pair<? extends Verifier, RPCBuffer> security = mSecurityHandler.initializeContext(request, requestBuffer);
            LOGGER.info(mSessionID + " Handling NFS NULL for GSS Procedure for " + mClientName);
            RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
            response.setReplyState(RPC_REPLY_STATE_ACCEPT);
            response.setAcceptState(RPC_ACCEPT_SUCCESS);
            response.setVerifier(security.getFirst());
            writeRPCResponse(response, security.getSecond());
          } else {
            LOGGER.info(mSessionID + " Handling NFS NULL Procedure for " + mClientName);
            RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
            response.setReplyState(RPC_REPLY_STATE_ACCEPT);
            response.setAcceptState(RPC_ACCEPT_SUCCESS);
            response.setVerifier(new VerifierNone());
            writeRPCResponse(response);
          }
        } else if(!(request.getCredentials() instanceof AuthenticatedCredentials)) {
          LOGGER.info(mSessionID + " Denying client due to non-authenticated credentials for " + mClientName);
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_REJECT_AUTH_ERROR);
          response.setAuthState(RPC_AUTH_STATUS_TOOWEAK);
          response.setVerifier(new VerifierNone());
          writeRPCResponse(response);
        } else if(!mSecurityHandler.hasAcceptableSecurity(request)) {
          LOGGER.info(mSessionID + " Denying client due to unacceptable credentials for " + mClientName);
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_REJECT_AUTH_ERROR);
          response.setAuthState(RPC_AUTH_STATUS_TOOWEAK);
          response.setVerifier(new VerifierNone());
          writeRPCResponse(response);
        } else if(request.getProcedure() == NFS_PROC_COMPOUND) {

          if(LOGGER.isDebugEnabled()) {
            LOGGER.debug(mSessionID + " Handling NFS Compound for " + mClientName);
          }

          if(mSecurityHandler.isUnwrapRequired()) {
            byte[] encryptedData = requestBuffer.readBytes();
            byte[] plainData = mSecurityHandler.unwrap(encryptedData);
            requestBuffer = new RPCBuffer(plainData);
          }
          boolean inProgress = mRequestsInProgress.putIfAbsent(request.getXid(), System.currentTimeMillis()) != null;
          if(inProgress) {
            mRetransmits++;
            mHandler.incrementMetric(RESTRANSMITS, 1);
            LOGGER.info(mSessionID + " ignoring request " + request.getXidAsHexString());
          } else {
            mRetransmits--;
            /*
             * TODO ensure credentials are the same for request/cached
             * response.
             */
            MessageBase applicationResponse = mResponseCache.get(request.getXid());
            if(applicationResponse != null) {
              writeApplicationResponse(request, applicationResponse);
              LOGGER.info(mSessionID + " serving cached response to " + request.getXidAsHexString());
            } else {
              execute(request, requestBuffer);
            }
          }
        } else {
          throw new UnsupportedOperationException("Unknown Proc " + request.getProcedure());
        }
      }
    } catch(EOFException e) {
      LOGGER.info(mSessionID + " Shutdown worker for client " + mClientName);
    } catch (Exception e) {
      LOGGER.error(mSessionID + " Error from client " + mClientName, e);
      if(request != null) {
        try {
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_ACCEPT_SYSTEM_ERR);
          writeRPCResponse(response);
        } catch (Exception x) {
          LOGGER.error(mSessionID + " Error writing failure packet", x);
        }
      }
    } finally {
      if (mOutputHandler != null) {
        mOutputHandler.close();
      }
      IOUtils.closeSocket(mClient);
      Map<Socket, ClientInputHandler<REQUEST, RESPONSE>> clients = mRPCServer.getClients();
      clients.remove(mClient);
    }
  }

  public void shutdown() {
    this.interrupt();
  }
  protected void writeApplicationResponse(RPCRequest request, MessageBase applicationResponse) {
    mResponseCache.put(request.getXid(), applicationResponse);
    LOGGER.info(mSessionID + " " + request.getXidAsHexString() + " Writing " + 
        applicationResponse.getClass().getSimpleName() + " to "  + mClientName);
    RPCBuffer responseBuffer = new RPCBuffer();
    // save space for length header
    responseBuffer.writeInt(Integer.MAX_VALUE);
    try {
      RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
      response.setReplyState(RPC_REPLY_STATE_ACCEPT);
      response.setAcceptState(RPC_ACCEPT_SUCCESS);
      response.setVerifier(mSecurityHandler.getVerifer(request));
      response.write(responseBuffer);
  
      if(mSecurityHandler.isWrapRequired()) {
        byte[] data = mSecurityHandler.wrap(applicationResponse);
        responseBuffer.writeUint32(data.length);
        responseBuffer.writeBytes(data);
      } else {
        applicationResponse.write(responseBuffer);
      }
      responseBuffer.flip();
      while(!mOutputBufferQueue.offer(responseBuffer)) {
        try {
          TimeUnit.SECONDS.sleep(1L);
        } catch (InterruptedException e) {
          // ignore, just sleep
        }
      }
      mHandler.incrementMetric(CLIENT_BYTES_WROTE, responseBuffer.length());
    } catch(NFS4Exception ex) {
      // TODO pass back error code
      LOGGER.error(mSessionID + " Error from client " + mClientName + " xid = " + request.getXid(), ex);
      try {
        RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
        response.setReplyState(RPC_REPLY_STATE_DENIED);
        response.setAcceptState(RPC_ACCEPT_SYSTEM_ERR);
        writeRPCResponse(response);
      } catch (Exception x) {
        LOGGER.error(mSessionID + " Error writing failure packet", x);
      }
    } finally {
      Long startTime = mRequestsInProgress.remove(request.getXid());
      if(startTime != null) {
        
      }
    }
  }
  protected void writeRPCResponse(RPCResponse response) {
    writeRPCResponse(response, null);
  }
  protected void writeRPCResponse(RPCResponse response, RPCBuffer payload) {
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug(mSessionID + " Writing bare RPC Response to "  + mClientName);
    }
    RPCBuffer responseBuffer = new RPCBuffer();
    // save space for length
    responseBuffer.writeInt(Integer.MAX_VALUE);
    response.write(responseBuffer);
    if(payload != null) {
      payload.flip();
      responseBuffer.writeRPCBUffer(payload);
    }
    responseBuffer.flip();
    while(!mOutputBufferQueue.offer(responseBuffer)) {
      try {
        TimeUnit.SECONDS.sleep(1L);
      } catch (InterruptedException e) {
        // ignore, just sleep
      }
    }
    mHandler.incrementMetric(CLIENT_BYTES_WROTE, responseBuffer.length());
  }

  private void execute(final RPCRequest request, RPCBuffer requestBuffer) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(mSessionID + " starting xid " + request.getXidAsHexString());
    }
    REQUEST applicationRequest = mHandler.createRequest();
    applicationRequest.read(requestBuffer);
    if (applicationRequest instanceof RequiresCredentials) {
      RequiresCredentials requiresCredentials = (RequiresCredentials) applicationRequest;
      // check to ensure it's auth creds is above
      requiresCredentials.setCredentials((AuthenticatedCredentials) request.getCredentials());
    }
    final ListenableFuture<RESPONSE> future = mHandler.process(request, applicationRequest, mClient.getInetAddress(), mSessionID);
    future.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          writeApplicationResponse(request, future.get());
        } catch (Throwable t) {
          LOGGER.error("Unexpected error processing request", t);
        }
      }
    }, MoreExecutors.sameThreadExecutor());
  }
}
