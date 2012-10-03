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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Responsible for queuing writes until prerequisite writes become available and
 * processing the writes when prerequisite writes are available. This is
 * required because a the NFS server event writes from a client writing
 * sequentially arrive wildly out of order.
 */
public class WriteOrderHandler extends Thread {

  protected static final Logger LOGGER = Logger.getLogger(WriteOrderHandler.class);
  /**
   * 500,0000 * 8 bytes = 3.8MB
   */
  protected static final int MAX_WRITE_IDS = 500000;
  protected FSDataOutputStream mOutputStream;
  protected ConcurrentMap<Long, Write> mPendingWrites = Maps.newConcurrentMap();
  protected AtomicLong mPendingWritesSize = new AtomicLong(0);
  protected List<Integer> mProcessedWrites = Lists.newArrayList();
  protected BlockingQueue<Write> mWriteQueue = new LinkedBlockingQueue<Write>();
  protected IOException mIOException;
  protected NFS4Exception mNFSException;
  protected AtomicLong mExpectedLength = new AtomicLong(0);
  protected AtomicBoolean mClosed = new AtomicBoolean(false);

  public WriteOrderHandler(FSDataOutputStream outputStream) throws IOException {
    mOutputStream = outputStream;
    mExpectedLength.set(mOutputStream.getPos());
  }

  public long getCurrentPos() throws IOException {
    return mOutputStream.getPos();
  }

  @Override
  public void run() {
    try {
      while (true) {
        Write write = mWriteQueue.poll(10, TimeUnit.SECONDS);
        if (write == null) {
          synchronized (mPendingWrites) {
            LOGGER.info("Pending Write Offsets: " + new TreeSet<Long>(mPendingWrites.keySet()));
          }
        } else {
          checkWriteState(write);
          mPendingWrites.put(write.offset, write);
          mPendingWritesSize.addAndGet(write.size);
        }
        if(LOGGER.isDebugEnabled()) {
          LOGGER.debug("Pending writes " + (mPendingWritesSize.get() / 1024L / 1024L) + "MB, current offset = " + getCurrentPos());
        }
        synchronized (mOutputStream) {
          checkPendingWrites();
        }
      }
    } catch (InterruptedException e) {
      LOGGER.info("Thread moving on due to interrupt");
    } catch (IOException e) {
      LOGGER.info("Thread quitting due to IO Error", e);
      mIOException = e;
    } catch (NFS4Exception e) {
      LOGGER.info("Thread quitting due NFS Exception", e);
      mNFSException = e;
    }
  }
  // turns out NFS clients will send the same write twice if the write rate is slow

  protected void checkWriteState(Write write) throws NFS4Exception {
    Write other = mPendingWrites.get(write.offset);
    if (other != null && !write.equals(other)) {
      throw new NFS4Exception(NFS4ERR_PERM, "Unable to process write (random write) at "
          + write.offset
          + ", sync = " + write.sync
          + ", length = " + write.length);
    }
  }

  protected void checkPendingWrites() throws IOException {
    Write write = null;
    synchronized (mPendingWrites) {
      while ((write = mPendingWrites.remove(mOutputStream.getPos())) != null) {
        mPendingWritesSize.addAndGet(-write.size);
        doWrite(write);
      }
    }
  }

  protected void doWrite(Write write) throws IOException {
    long preWritePos = mOutputStream.getPos();
    checkState(preWritePos == write.offset, " Offset = " + write.offset + ", pos = " + preWritePos);
    mOutputStream.write(write.data, write.start, write.length);
    LOGGER.info("Writing to " + write.name + " " + write.offset + ", "
        + write.length + ", new offset = " + mOutputStream.getPos() + ", hash = " + write.hashCode);
    if (write.sync) {
      mOutputStream.sync();
    }
  }

  protected void checkException() throws IOException, NFS4Exception {
    if (mNFSException != null) {
      throw mNFSException;
    }
    if (mIOException != null) {
      throw mIOException;
    }
    if (!this.isAlive()) {
      throw new NFS4Exception(NFS4ERR_PERM, "WriteOrderHandler is dead");
    }
  }

  /**
   * Block until the outputstream reaches the specified offset.
   *
   * @param offset
   * @throws IOException thrown if the underlying stream throws an IOException
   * @throws NFS4Exception thrown if the thread processing writes dies
   */
  public void sync(long offset) throws IOException, NFS4Exception {
    LOGGER.info("Sync for " + mOutputStream + " and " + offset + 
        ", pending writes = " + mPendingWrites.keySet() + ", write queue = " + mWriteQueue.size());
    while (mOutputStream.getPos() < offset) {
      checkException();
      pause(10L);
    }
    synchronized (mOutputStream) {
      mOutputStream.sync();
    }
  }
  public boolean syncWouldBlock(long offset) throws IOException {
    return writeWouldBlock(offset, true);
  }
  public boolean writeWouldBlock(long offset, boolean sync) throws IOException {
    if(sync && mOutputStream.getPos() < offset) {
      return true;
    }
    return false;
  }
  public boolean closeWouldBlock() throws IOException {
    LOGGER.info("Close would block for " + mOutputStream + ", expectedLength = " + mExpectedLength.get() + 
        ", mOutputStream.getPos = " + mOutputStream.getPos() + ", pending writes = " + mPendingWrites.keySet() + 
        ", write queue = " + mWriteQueue.size());
    if(mOutputStream.getPos() < mExpectedLength.get()
        || !(mPendingWrites.isEmpty() && mWriteQueue.isEmpty())) {
      return true;
    }
    return false;
  }
  /**
   * Close the underlying stream blocking until all writes have been
   * processed.
   *
   * @throws IOException thrown if the underlying stream throws an IOException
   * @throws NFS4Exception thrown if the thread processing writes dies
   */
  public void close() throws IOException, NFS4Exception {
    mClosed.set(true);
    while (closeWouldBlock()) {
      pause(1000L);
      sync(mExpectedLength.get());
    }
    synchronized (mOutputStream) {
      mOutputStream.sync();
      mOutputStream.close();
    }
    checkState(mPendingWrites.isEmpty(), "Pending writes for " + mOutputStream + " at "
        + mOutputStream.getPos() + " = " + mPendingWrites);
    LOGGER.info("Closing " + mOutputStream + " at " + mOutputStream.getPos());
    this.interrupt();
  }

  protected void pause(long ms) throws IOException {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while paused", e);
    }
  }

  /**
   * Queues write of the specified bytes for the underlying outputstream.
   *
   * @param xid
   * @param offset
   * @param sync
   * @param data
   * @param start
   * @param length
   * @return
   * @throws IOException if file is closed or underlying stream throws
   * IOException or if the thread is interrupted while putting the write on
   * the queue
   * @throws NFS4Exception if random write
   */
  public int write(String name, int xid, long offset, boolean sync, byte[] data, int start, int length)
      throws IOException, NFS4Exception {
    checkException();
    if (mClosed.get()) {
      throw new IOException("File closed");
    }
    try {
      // check to ensure we haven't already processed this write
      // this happens when writes are retransmitted but not in
      // our response cache. Surprisingly often.
      synchronized (mProcessedWrites) {
        if (mProcessedWrites.contains(xid)) {
          LOGGER.info("Write already processed " + xid);
          return length;
        }
        if(mProcessedWrites.size() >= MAX_WRITE_IDS) {
          mProcessedWrites.remove(0);
        }
        mProcessedWrites.add(xid);
        if (offset < mOutputStream.getPos()) {
          throw new NFS4Exception(NFS4ERR_PERM, "Unable to process write (random write) at "
              + offset
              + ", pos = " + mOutputStream.getPos()
              + ", sync = " + sync
              + ", length = " + length);
        }
        synchronized (mExpectedLength) {
          if (offset > mExpectedLength.get()) {
            mExpectedLength.set(offset);
          }
        }
        mWriteQueue.put(new Write(name, xid, offset, sync, data, start, length));
      }
      if (sync) {
        sync(offset); // blocks
      }
      return length;
    } catch (InterruptedException e) {
      throw new IOException("Interrupted While Putting Write", e);
    }
  }

  protected static class Write {

    String name;
    int xid;
    long offset;
    boolean sync;
    byte[] data;
    int start;
    int length;
    int hashCode;
    int size;

    public Write(String name, int xid, long offset, boolean sync, byte[] data, int start, int length) {
      this.name = name;
      this.xid = xid;
      this.offset = offset;
      this.sync = sync;
      this.data = data;
      this.start = start;
      this.length = length;
      this.hashCode = hashCode();
      this.size = getSize();
    }

    public int getSize() {
      int size = 4; // obj header     
      size += name.length() + 4; // string, 4 byte length?
      size += 4; // xid
      size += 8; // offset
      size += 1; // sync
      size += data.length + 4; // data
      size += 4; // start
      size += 4; // length
      size += 4; // hashcode
      size += 4; // size
      return size;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (offset ^ (offset >>> 32));
      result = prime * result + hashBytes(data, start, length);
      return result;
    }

    protected static int hashBytes(byte[] bytes, int offset, int length) {
      int hash = 1;
      for (int i = offset; i < offset + length; i++) {
        hash = (31 * hash) + (int) bytes[i];
      }
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Write other = (Write) obj;
      if (offset != other.offset) {
        return false;
      }
      return Bytes.compareTo(data, start, length, other.data, other.start, other.length) == 0;
    }
  }
}
