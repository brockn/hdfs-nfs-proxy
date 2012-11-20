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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RequiresCredentials;
import com.cloudera.hadoop.hdfs.nfs.security.AccessPrivilege;
import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;
import com.cloudera.hadoop.hdfs.nfs.security.SecurityHandlerFactory;
import com.cloudera.hadoop.hdfs.nfs.security.SessionSecurityHandler;
import com.cloudera.hadoop.hdfs.nfs.security.Verifier;
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

  private static final Logger LOGGER = Logger.getLogger(ClientInputHandler.class);
  private static final AtomicInteger SESSIONID = new AtomicInteger(Integer.MAX_VALUE);

  private final Socket mClient;
  private final String mClientName;
  private final RPCServer<REQUEST, RESPONSE> mRPCServer;
  private final BlockingQueue<RPCBuffer> mOutputBufferQueue;
  private final RPCHandler<REQUEST, RESPONSE> mHandler;
  private final SecurityHandlerFactory mSecurityHandlerFactory;
  private final ConcurrentMap<Integer, Long> mRequestsInProgress;
  private final Map<Integer, MessageBase> mResponseCache;
  protected final String mSessionID;
  protected final Configuration mConfiguration;

  private ClientOutputHandler mOutputHandler;
  /**
   * Number of retransmits received
   */
  protected long mRetransmits = 0L;

  public ClientInputHandler(Configuration conf, RPCServer<REQUEST, RESPONSE> server,
      RPCHandler<REQUEST, RESPONSE> handler, SecurityHandlerFactory securityHandlerFactory, Socket client) {
    mConfiguration = conf;
    mRPCServer = server;
    mHandler = handler;
    mClient = client;
    mSecurityHandlerFactory = securityHandlerFactory;
    String clientHost = mClient.getInetAddress().getCanonicalHostName();
    mClientName = clientHost + ":" + mClient.getPort();
    mSessionID = "0x" + Integer.toHexString(SESSIONID.addAndGet(-5));
    setName("RPCServer-" + mClientName);
    mOutputBufferQueue = new LinkedBlockingQueue<RPCBuffer>(1000);
    mRequestsInProgress = mRPCServer.getRequestsInProgress();
    mResponseCache = mRPCServer.getResponseCache();
  }

  private void execute(final SessionSecurityHandler<? extends Verifier> securityHandler,
      AccessPrivilege accessPrivilege, final RPCRequest request, RPCBuffer requestBuffer) throws Exception {
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
    final ListenableFuture<RESPONSE> future = mHandler.process(request, applicationRequest,
        accessPrivilege, mClient.getInetAddress(), mSessionID);
    future.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          writeApplicationResponse(securityHandler, request, future.get());
        } catch (Throwable t) {
          LOGGER.error("Unexpected error processing request", t);
        }
      }
    }, MoreExecutors.sameThreadExecutor());
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
        mHandler.incrementMetric(CLIENT_BYTES_READ, requestBuffer.length());
        request = new RPCRequest();
        request.read(requestBuffer);
        AccessPrivilege accessPrivilege = mSecurityHandlerFactory.getAccessPrivilege(mClient.getInetAddress());
        if(request.getRpcVersion() != RPC_VERSION) {
          LOGGER.info(mSessionID + " Denying client due to bad RPC version " + request.getRpcVersion() + " for " + mClientName);
          throw new RPCDeniedException(RPC_REJECT_MISMATCH);
        } else if(accessPrivilege == AccessPrivilege.NONE) {
          throw new RPCAuthException(RPC_AUTH_STATUS_BADCRED);
        } else if(request.getCredentials() == null) {
          LOGGER.info(mSessionID + " Denying client due to null credentials for " + mClientName);
          throw new RPCAuthException(RPC_AUTH_STATUS_TOOWEAK);
        } else if(request.getProcedure() == NFS_PROC_NULL) {
          writeRPCBuffer(mSecurityHandlerFactory.
              handleNullRequest(mSessionID, mClientName, request, requestBuffer));
        } else if(!(request.getCredentials() instanceof AuthenticatedCredentials)) {
          LOGGER.info(mSessionID + " Denying client due to non-authenticated credentials for " + mClientName);
          throw new RPCAuthException(RPC_AUTH_STATUS_TOOWEAK);
        } else if(request.getProcedure() == NFS_PROC_COMPOUND) {
          if(LOGGER.isDebugEnabled()) {
            LOGGER.debug(mSessionID + " Handling NFS Compound for " + mClientName);
          }
          SessionSecurityHandler<? extends Verifier> securityHandler =
              mSecurityHandlerFactory.getSecurityHandler(request.getCredentials());
          if(!securityHandler.shouldSilentlyDrop(request)) {
            if(securityHandler.isUnwrapRequired()) {
              byte[] encryptedData = requestBuffer.readBytes();
              requestBuffer = securityHandler.unwrap(request, encryptedData);
            }
            // if putIfAbsent returns non-null then the request is already in progress
            boolean inProgress = mRequestsInProgress.putIfAbsent(request.getXid(),
                System.currentTimeMillis()) != null;
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
                writeApplicationResponse(securityHandler, request, applicationResponse);
                LOGGER.info(mSessionID + " serving cached response to " + request.getXidAsHexString());
              } else {
                execute(securityHandler, accessPrivilege, request, requestBuffer);
              }
            }
          }
        } else {
          throw new UnsupportedOperationException("Unknown Proc " + request.getProcedure());
        }
      }
    } catch(EOFException e) {
      LOGGER.info(mSessionID + " Shutdown worker for client " + mClientName);
    } catch (RPCException e) {
      handleRPCException(request, e);
    } catch (Exception e) {
      LOGGER.error(mSessionID + " Error from client " + mClientName, e);
      if(request != null) {
        try {
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_ACCEPT);
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
      // pause for output handler drain it's queue
      try {
        Thread.sleep(1500L);
      } catch (InterruptedException e) {
        LOGGER.info("Interrupted while pausing for output handler");
      }
      IOUtils.closeSocket(mClient);
      Map<Socket, ClientInputHandler<REQUEST, RESPONSE>> clients = mRPCServer.getClients();
      clients.remove(mClient);
    }
  }
  private void handleRPCException(RPCRequest request, RPCException e) {
    LOGGER.error(mSessionID + " RPCException for client " + mClientName, e);
    if(request != null) {
      try {
        RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
        response.setReplyState(e.getReplyState());
        response.setAcceptState(e.getAcceptState());
        if(e instanceof RPCAuthException) {
          response.setAuthState(((RPCAuthException)e).getAuthState());
        }
        writeRPCResponse(response);
      } catch (Exception x) {
        LOGGER.error(mSessionID + " Error writing RPC Exception packet", x);
      }
    }
  }
  public void shutdown() {
    this.interrupt();
  }
  protected void writeApplicationResponse(SessionSecurityHandler<? extends Verifier> securityHandler,
      RPCRequest request, MessageBase applicationResponse) {
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
      response.setVerifier(securityHandler.getVerifer(request));
      response.write(responseBuffer);
      if(securityHandler.isWrapRequired()) {
        byte[] data = securityHandler.wrap(request, applicationResponse);
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
    } catch(RPCException e) {
      handleRPCException(request, e);
    } finally {
      Long startTime = mRequestsInProgress.remove(request.getXid());
      if(startTime != null) {
        mHandler.incrementMetric(COMPOUND_REQUEST_ELAPSED_TIME, System.currentTimeMillis() - startTime);
      }
    }
  }
  protected void writeRPCBuffer(RPCBuffer responseBuffer) {
    while(!mOutputBufferQueue.offer(responseBuffer)) {
      try {
        TimeUnit.SECONDS.sleep(1L);
      } catch (InterruptedException e) {
        // ignore, just sleep
      }
    }
    mHandler.incrementMetric(CLIENT_BYTES_WROTE, responseBuffer.length());
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
    writeRPCBuffer(responseBuffer);
  }
}
