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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.security.SecurityHandlerFactory;
import com.google.common.collect.Maps;

public class RPCServer<REQUEST extends MessageBase, RESPONSE extends MessageBase> extends Thread {

  protected static final Logger LOGGER = Logger.getLogger(RPCServer.class);
  protected RPCHandler<REQUEST, RESPONSE> mHandler;
  protected ConcurrentMap<Socket, ClientInputHandler<REQUEST, RESPONSE>> mClients = Maps.newConcurrentMap();
  protected int mPort;
  protected ServerSocket mServer;
  protected Configuration mConfiguration;
  protected Map<Integer, MessageBase> mResponseCache =
      Collections.synchronizedMap(new LRUCache<Integer, MessageBase>(500));
  protected ConcurrentMap<Integer, Long> mRequestsInProgress = Maps.newConcurrentMap();
  private final SecurityHandlerFactory mSecurityHandlerFactory;

  public RPCServer(RPCHandler<REQUEST, RESPONSE> rpcHandler, Configuration conf, InetAddress address) throws Exception {
    this(rpcHandler, conf, address, 0);
  }

  public RPCServer(RPCHandler<REQUEST, RESPONSE> rpcHandler, Configuration conf, InetAddress address, int port) throws IOException {
    mHandler = rpcHandler;
    mConfiguration = conf;
    mPort = port;
    mSecurityHandlerFactory = new SecurityHandlerFactory(mConfiguration);
    mServer = new ServerSocket(mPort, -1, address);
    // if port is 0, we are supposed to find a port
    // mPort should then be set to the port we found
    mPort = mServer.getLocalPort();
    setName("RPCServer-" + mHandler.getClass().getSimpleName() + "-" + mPort);
  }

  @Override
  public void run() {
    try {
      mServer.setReuseAddress(true);
      mServer.setPerformancePreferences(0, 1, 0);
      LOGGER.info(mHandler.getClass() + " created server " + mServer + " on " + mPort);

      while (true) {
        Socket client = mServer.accept();
        String name = client.getInetAddress().getCanonicalHostName() + ":" + client.getPort();
        LOGGER.info(mHandler.getClass() + " got client " + name);

        ClientInputHandler<REQUEST, RESPONSE> worker = new ClientInputHandler<REQUEST, RESPONSE>(mConfiguration, this, mHandler, 
            mSecurityHandlerFactory, client);
        mClients.put(client, worker);
        worker.start();
      }
    } catch (Exception ex) {
      LOGGER.error("Error on handler " + mHandler.getClass(), ex);
    } finally {
      shutdown();
    }
  }

  public void shutdown() {
    // first close clients
    for (Socket client : mClients.keySet()) {
      ClientInputHandler<REQUEST, RESPONSE> worker = mClients.get(client);
      if (worker != null && worker.isAlive()) {
        worker.shutdown();
      }
      IOUtils.closeSocket(client);
    }
    // then server
    if (mServer != null) {
      try {
        mServer.close();
      } catch (Exception e) {
      }
    }
  }

  public int getPort() {
    return mPort;
  }

  protected Map<Integer, MessageBase> getResponseCache() {
    return mResponseCache;
  }

  public ConcurrentMap<Integer, Long> getRequestsInProgress() {
    return mRequestsInProgress;
  }

  public ConcurrentMap<Socket, ClientInputHandler<REQUEST, RESPONSE>> getClients() {
    return mClients;
  }
}
