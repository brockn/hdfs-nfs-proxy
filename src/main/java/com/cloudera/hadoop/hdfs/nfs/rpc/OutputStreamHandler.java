package com.cloudera.hadoop.hdfs.nfs.rpc;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OutputStreamHandler extends Thread {
  protected static final Logger LOGGER = LoggerFactory.getLogger(OutputStreamHandler.class);
  protected BlockingQueue<RPCBuffer> mQueue = new ArrayBlockingQueue<RPCBuffer>(10, true);
  protected OutputStream mOutputStream;
  protected IOException mException;
  protected String mClientName;
  public OutputStreamHandler(OutputStream outputStream, String client) {
    mOutputStream = outputStream;
    mClientName = client;
  }
  public void close() {
    this.interrupt();
  }
  public void add(RPCBuffer buffer) throws IOException {
    if(mException != null) {
      IOException copy = mException;
      mException = null;
      throw copy;
    }
    try {
      mQueue.put(buffer);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  public void run() {
    while(true) {
      try {
        try {
          mQueue.take().write(mOutputStream);
        } catch (InterruptedException e) {
          LOGGER.info("OutputHandler for " + mClientName + " quitting.");
          break;
        }
      } catch (IOException e) {
        mException = e;
      }
    }
  }
}