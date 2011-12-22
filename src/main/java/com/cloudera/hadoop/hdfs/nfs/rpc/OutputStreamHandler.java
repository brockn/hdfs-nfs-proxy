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
  protected IOException mException;
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
    while(!mShutdown) {
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