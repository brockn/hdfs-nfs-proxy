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
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * Thread which writes RPCBuffer's to an OutputStream. RPCBuffers to be written
 * should be added to the queue by the add(buffer) method. The method is thread
 * safe.
 */
class ClientOutputHandler extends Thread {

  protected static final Logger LOGGER = Logger.getLogger(ClientOutputHandler.class);
  protected BlockingQueue<RPCBuffer> mWorkQueue;
  protected OutputStream mOutputStream;
  protected String mClientName;
  protected volatile boolean mShutdown;

  /**
   * OutputStream the thread will write to and a client name which is used for
   * debug logging on error writing to stream.
   *
   * @param outputStream
   * @param client
   */
  public ClientOutputHandler(OutputStream outputStream, BlockingQueue<RPCBuffer> workQueue, String client) {
    mOutputStream = outputStream;
    mWorkQueue = workQueue;
    mClientName = client;
    mShutdown = false;
    setName("OutputStreamHandler-" + mClientName);
  }

  /**
   * Stop the thread writing to the OutputStream.
   */
  public void close() {
    mShutdown = true;
  }

  protected void put(RPCBuffer buffer) {
    try {
      mWorkQueue.put(buffer);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void run() {
    while (!mShutdown) {
      RPCBuffer buffer = null;
      try {
        buffer = mWorkQueue.poll(500L, TimeUnit.MILLISECONDS);
        if (buffer != null) {
          buffer.write(mOutputStream);
          mOutputStream.flush();
        }
        buffer = null;
      } catch (IOException e) {
        LOGGER.warn("OutputStreamHandler for " + mClientName + " got error on write", e);
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException x) {
          Thread.currentThread().interrupt();
        }
      } catch (InterruptedException e) {
        LOGGER.info("OutputStreamHandler for " + mClientName + " interrupted");
        Thread.currentThread().interrupt();
      } finally {
        if (buffer != null) {
          put(buffer);
        }
      }
    }
    // Process the remaining queue
    try {
      while(true) {
        RPCBuffer buffer = mWorkQueue.poll();
        if (buffer == null) {
          break;
        }
        buffer.write(mOutputStream);
      }
    } catch (IOException e) {
      LOGGER.info("OutputStreamHandler for " + mClientName + 
          " got error on final write: " + e.getMessage());
    }
  }
}