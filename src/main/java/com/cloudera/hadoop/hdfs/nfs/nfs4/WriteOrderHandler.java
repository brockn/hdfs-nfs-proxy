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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_PERM;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSOutputStream;
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
  protected HDFSOutputStream mOutputStream;
  protected ConcurrentMap<Long, PendingWrite> mPendingWrites = Maps.newConcurrentMap();
  protected AtomicLong mPendingWritesSize = new AtomicLong(0);
  protected List<Integer> mProcessedWrites = Lists.newArrayList();
  protected BlockingQueue<PendingWrite> mWriteQueue = new LinkedBlockingQueue<PendingWrite>();
  protected IOException mIOException;
  protected NFS4Exception mNFSException;
  protected AtomicLong mExpectedLength = new AtomicLong(0);
  protected AtomicBoolean mClosed = new AtomicBoolean(false);
  private String identifer;
  
  public WriteOrderHandler(HDFSOutputStream outputStream) throws IOException {
    mOutputStream = outputStream;
    mExpectedLength.set(mOutputStream.getPos());
    identifer = UUID.randomUUID().toString();
  }

  public String getIdentifer() {
    return identifer;
  }
  public long getCurrentPos() {
    return mOutputStream.getPos();
  }

  @Override
  public void run() {
    try {
      while (true) {
        PendingWrite write = mWriteQueue.poll(10, TimeUnit.SECONDS);
        if (write == null) {
          synchronized (mPendingWrites) {
            SortedSet<Long> offsets = new TreeSet<Long>(mPendingWrites.keySet());
            if(!offsets.isEmpty()) {
              LOGGER.info("Pending Write Offsets " + offsets.size() + ": first = " + 
                  offsets.first() +  ", last = " + offsets.last());
            }
          }
        } else {
          checkWriteState(write);
          mPendingWrites.put(write.getOffset(), write);
          mPendingWritesSize.addAndGet(write.getSize());
        }
        if(mPendingWritesSize.get() > 0L) {
          LOGGER.info("Pending writes " + (mPendingWritesSize.get() / 1024L / 1024L) + "MB, " +
          		"current offset = " + getCurrentPos());          
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

  protected void checkWriteState(PendingWrite write) throws NFS4Exception {
    PendingWrite other = mPendingWrites.get(write.getOffset());
    if (other != null && !write.equals(other)) {
      throw new NFS4Exception(NFS4ERR_PERM, "Unable to process write (random write) at "
          + write.getOffset()
          + ", sync = " + write.isSync()
          + ", length = " + write.getLength());
    }
  }

  protected void checkPendingWrites() throws IOException {
    PendingWrite write = null;
    while ((write = mPendingWrites.remove(mOutputStream.getPos())) != null) {
      mPendingWritesSize.addAndGet(-write.getSize());
      doWrite(write);
    }
  }

  protected void doWrite(PendingWrite write) throws IOException {
    long preWritePos = mOutputStream.getPos();
    checkState(preWritePos == write.getOffset(), " Offset = " + write.getOffset() + ", pos = " + preWritePos);
    mOutputStream.write(write.getData(), write.getStart(), write.getLength());
    if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("Writing to " + write.getName() + " " + write.getOffset() + ", "
            + write.getLength() + ", new offset = " + mOutputStream.getPos() + ", hash = " + write.hashCode());
    }
    if (write.isSync()) {
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
    String pendingWrites;
    synchronized (mPendingWrites) {
      pendingWrites = mPendingWrites.keySet().toString();
    }
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("Sync for " + mOutputStream + " and " + offset + 
          ", pending writes = " + pendingWrites + ", write queue = " + mWriteQueue.size());      
    }
    while (mOutputStream.getPos() < offset) {
      checkException();
      pause(10L);
    }
    synchronized (mOutputStream) {
      mOutputStream.sync();
    }
  }
  /**
   * Check to see if a sync at offset would block
   * @param offset the sync is waiting for
   * @return true if the sync call would block
   * @throws IOException
   */
  public boolean syncWouldBlock(long offset) {
    return synchronousWriteWouldBlock(offset);
  }
  /**
   * Check to see if a write to offset with sync flag would block
   * @param offset the write occurs at
   * @return true if the write would block
   * @throws IOException
   */
  public boolean synchronousWriteWouldBlock(long offset) {
    if(getCurrentPos() < offset) {
      return true;
    }
    return false;
  }
  /**
   * Check to see if a close() call would block
   * @return true if a call to close would block
   * @throws IOException
   */
  public boolean closeWouldBlock() throws IOException {
    if(LOGGER.isDebugEnabled()) {
      String pendingWrites;
      synchronized (mPendingWrites) {
        pendingWrites = mPendingWrites.keySet().toString();
      }
      LOGGER.debug("Close would block for " + mOutputStream + ", expectedLength = " + mExpectedLength.get() + 
          ", mOutputStream.getPos = " + mOutputStream.getPos() + ", pending writes = " + pendingWrites + 
          ", write queue = " + mWriteQueue.size());
    }
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
  public int write(PendingWrite write)
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
        if (mProcessedWrites.contains(write.getXid())) {
          LOGGER.info("Write already processed " + write.getXidAsHexString());
          return write.getLength();
        }
        mProcessedWrites.add(write.getXid());
        if (write.getOffset() < mOutputStream.getPos()) {
          throw new NFS4Exception(NFS4ERR_PERM, "Unable to process write (random write) at "
              + write.getOffset()
              + ", pos = " + getCurrentPos()
              + ", sync = " + write.isSync()
              + ", length = " + write.getLength());
        }
        synchronized (mExpectedLength) {
          if (write.getOffset() > mExpectedLength.get()) {
            mExpectedLength.set(write.getOffset());
          }
        }
        mWriteQueue.put(write);
      }
      if (write.isSync()) {
        sync(write.getOffset()); // blocks
      }
      return write.getLength();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted While Putting Write", e);
    }
  }
}
