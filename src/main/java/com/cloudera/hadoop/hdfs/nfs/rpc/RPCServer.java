/**
 * Copyright 2011 The Apache Software Foundation
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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.google.common.collect.Maps;

public class RPCServer<REQUEST extends MessageBase, RESPONSE extends MessageBase> extends Thread {

  protected static final Logger LOGGER = Logger.getLogger(RPCServer.class);
  protected RPCHandler<REQUEST, RESPONSE> mHandler;
  protected ConcurrentMap<Socket, ClientWorker<REQUEST, RESPONSE>> mClients = Maps.newConcurrentMap();
  protected int mPort;
  protected ServerSocket mServer;
  protected Configuration mConfiguration;
  protected Map<Integer, MessageBase> mResponseCache =
      Collections.synchronizedMap(new LRUCache<Integer, MessageBase>(500));
  protected Set<Integer> mRequestsInProgress = Collections.synchronizedSet(new HashSet<Integer>());
  protected ExecutorService mExecutor;
  protected Map<String, BlockingQueue<RPCBuffer>> mOutputQueueMap = Maps.newHashMap();

  public RPCServer(RPCHandler<REQUEST, RESPONSE> rpcHandler, Configuration conf, InetAddress address) throws Exception {
    this(rpcHandler, conf, address, 0);
  }

  public RPCServer(RPCHandler<REQUEST, RESPONSE> rpcHandler, Configuration conf, InetAddress address, int port) throws IOException {
    mExecutor = new ThreadPoolExecutor(10, conf.getInt(RPC_MAX_THREADS, 500),
        30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

    mHandler = rpcHandler;
    mConfiguration = conf;
    mPort = port;
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

        ClientWorker<REQUEST, RESPONSE> worker = new ClientWorker<REQUEST, RESPONSE>(mConfiguration, this, mHandler, client);
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
      ClientWorker<REQUEST, RESPONSE> worker = mClients.get(client);
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

  protected BlockingQueue<RPCBuffer> getOutputQueue(String name) {
    synchronized (mOutputQueueMap) {
      if (!mOutputQueueMap.containsKey(name)) {
        mOutputQueueMap.put(name, new LinkedBlockingQueue<RPCBuffer>(1000));
      }
      return mOutputQueueMap.get(name);
    }
  }

  protected Map<Integer, MessageBase> getResponseCache() {
    return mResponseCache;
  }

  public Set<Integer> getRequestsInProgress() {
    return mRequestsInProgress;
  }

  public ConcurrentMap<Socket, ClientWorker<REQUEST, RESPONSE>> getClients() {
    return mClients;
  }

  public ExecutorService getExecutorService() {
    return mExecutor;
  }
}
