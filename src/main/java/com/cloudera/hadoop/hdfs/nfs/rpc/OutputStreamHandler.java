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
package com.cloudera.hadoop.hdfs.nfs.rpc;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread which writes RPCBuffer's to an OutputStream.
 * RPCBuffers to be written should be added to the queue
 * by the add(buffer) method. The method is thread safe.
 */
class OutputStreamHandler extends Thread {
  protected static final Logger LOGGER = LoggerFactory.getLogger(OutputStreamHandler.class);
  protected BlockingQueue<RPCBuffer> mQueue = new ArrayBlockingQueue<RPCBuffer>(10, true);
  protected OutputStream mOutputStream;
  protected String mClientName;
  protected volatile boolean mShutdown;
  /**
   * OutputStream the thread will write to and a client name
   * which is used for debug logging on error writing to stream.
   * @param outputStream
   * @param client
   */
  public OutputStreamHandler(OutputStream outputStream, String client) {
    mOutputStream = outputStream;
    mClientName = client;
    mShutdown = false;
    setName("OutputStreamHandler-" + mClientName);
  }
  /**
   * Stop the thread writing to the OutputStream.
   */
  public void close() {
    mShutdown = true;
    this.interrupt();
  }
  /**
   * Place buffer on an internal blocking queue to be written
   * later by the OutputStreamHandler. If OutputStreamHandler
   * encounters an error writing to the stream, the error will
   * be rethrown on the next call to the add(buffer) method.
   * @param buffer
   * @throws IOException
   */
  public void add(RPCBuffer buffer) throws IOException {
    if(mShutdown || !this.isAlive()) {
      throw new IOException("Write thread dead");
    }
    try {
      mQueue.put(buffer);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  public void run() {
    while(!mShutdown) {
      try {
        try {
          RPCBuffer buffer = mQueue.take();
          buffer.write(mOutputStream);
        } catch (InterruptedException e) {
          LOGGER.info("OutputStreamHandler for " + mClientName + " quitting.");
          break;
        }
      } catch (IOException e) {
        LOGGER.warn("OutputStreamHandler for " + mClientName + " got error on write", e);
      }
    }
    while(true) {
      try {
        RPCBuffer buffer = mQueue.poll();
        if(buffer == null) {
          break;
        }
        buffer.write(mOutputStream);
      } catch(Exception e) {
        LOGGER.warn("OutputStreamHandler for " + mClientName + " got error on final write", e);
      }
    }
  }
}