package com.cloudera.hadoop.hdfs.nfs.nfs4;


import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Responsible for queuing writes until prerequisite writes become
 * available and processing the writes when prerequisite writes
 * are available. This is required because a the NFS server
 * event writes from a client writing sequentially arrive wildly
 * out of order.
 */
public class WriteOrderHandler extends Thread {
  protected static final Logger LOGGER = LoggerFactory.getLogger(WriteOrderHandler.class);
  protected FSDataOutputStream mOutputStream;
  protected ConcurrentMap<Long, Write> mPendingWrites = Maps.newConcurrentMap();
  protected List<Integer> mProcessedWrites = Lists.newArrayList();
  protected BlockingQueue<Write> mWriteQueue = new LinkedBlockingQueue<Write>();
  protected IOException mIOException;
  protected NFS4Exception mNFSException;
  protected AtomicLong mExpectedLength = new AtomicLong(0);
  protected AtomicBoolean mClosed = new AtomicBoolean(false);
  
  public WriteOrderHandler(FSDataOutputStream outputStream) throws IOException {
    mOutputStream = outputStream;
    mExpectedLength.set(getCurrentPos());
  }
  
  public void run() {
    try {
      while(true) {
        Write write = checkWriteState(mWriteQueue.take());
        mPendingWrites.put(write.offset, write);
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
  protected Write checkWriteState(Write write) throws NFS4Exception {
    Write other = mPendingWrites.get(write.offset);
    if(other != null && !write.equals(other)) {
      throw new NFS4Exception(NFS4ERR_PERM, "Unable to process write (random write) at " + 
          write.offset + 
          ", sync = " + write.sync + 
          ", length = " + write.length);
    }
    return write;
  }
  protected void checkPendingWrites() throws IOException {
    Write write = null;
    synchronized (mPendingWrites) {
      while((write = mPendingWrites.remove(getCurrentPos())) != null) {
        doWrite(write);
      }      
    }
  }
  protected void doWrite(Write write) throws IOException {
    long preWritePos = getCurrentPos();
    checkState(preWritePos == write.offset, " Offset = " + write.offset + ", pos = " + preWritePos);
    mOutputStream.write(write.data, write.start, write.length);
    LOGGER.info("Writing to " + mOutputStream  + " " + write.offset + ", " + write.length + ", new offset = " + getCurrentPos());
    if(write.sync) {
      mOutputStream.sync();
    }
    synchronized(mProcessedWrites) {
      mProcessedWrites.add(write.xid);
    }
  }
  protected void checkException() throws IOException, NFS4Exception {
    if(mNFSException != null) {
      throw mNFSException;
    }
    if(mIOException != null) {
      throw mIOException;
    }
    if(!this.isAlive()) {
      throw new NFS4Exception(NFS4ERR_PERM, "WriteOrderHandler is dead");
    }
  }
  /**
   * Block until the outputstream reaches the specified offset. 
   * @param offset
   * @throws IOException thrown if the underlying stream throws an IOException
   * @throws NFS4Exception thrown if the thread processing writes dies
   */
  public void sync(long offset) throws IOException, NFS4Exception {
    LOGGER.info("Sync for " + mOutputStream + " and " + offset);
    while(getCurrentPos() < offset) {
      checkException();
      pause(10L);
    }
  }
  /**
   * Close the underlying stream blocking until all writes have been processed.
   * 
   * @throws IOException thrown if the underlying stream throws an IOException
   * @throws NFS4Exception thrown if the thread processing writes dies
   */
  public void close() throws IOException, NFS4Exception {
    mClosed.set(true);
    while(getCurrentPos() < mExpectedLength.get() ||
        !(mPendingWrites.isEmpty() && mWriteQueue.isEmpty())) {
      pause(10L);
      sync(mExpectedLength.get());
    }
    synchronized (mOutputStream) {
      mOutputStream.sync();
      mOutputStream.close();
    }
    checkState(mPendingWrites.isEmpty(), "Pending writes for " + mOutputStream + " at " + 
        getCurrentPos() + " = " + mPendingWrites);    
    LOGGER.info("Closing " + mOutputStream + " at " + getCurrentPos());
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
   * Get the current position of the output stream.
   * @return
   * @throws IOException
   */
  public long getCurrentPos() throws IOException {
    synchronized(mOutputStream) {
      return mOutputStream.getPos();
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
   * @throws IOException if file is closed or underlying stream throws IOException
   * or if the thread is interrupted while putting the write on the queue
   * @throws NFS4Exception if random write
   */
  public int write(int xid, long offset, boolean sync, byte[] data, int start, int length) 
      throws IOException, NFS4Exception {
    checkException();
    if(mClosed.get()) {
      throw new IOException("File closed");
    }
    // check to ensure we haven't already processed this write
    // this happens when writes are retransmitted but not in 
    // our response cache. Surprisingly often at scale.
    synchronized(mProcessedWrites) {
      if(mProcessedWrites.contains(xid)) {
        LOGGER.info("Write already processed " + xid);
        return length;
      }
    }
    if(offset < getCurrentPos()) {
      throw new NFS4Exception(NFS4ERR_PERM, "Unable to process write (random write) at " + 
          offset + 
          ", pos = " + getCurrentPos() +
          ", sync = " + sync + 
          ", length = " + length);
    }
    try {
      synchronized (mExpectedLength) {
        if(offset > mExpectedLength.get()) {
          mExpectedLength.set(offset);
        }
      }
      mWriteQueue.put(new Write(xid, offset, sync, data, start, length));
      if(sync) {
        sync(offset);
      }
      return length;
    } catch (InterruptedException e) {
      throw new IOException("Interrupted While Putting Write", e);
    }
  }
  
  protected static class Write {
    int xid;
    long offset;
    boolean sync;
    byte[] data;
    int start;
    int length;
    public Write(int xid, long offset, boolean sync, byte[] data, int start, int length) {
      this.xid = xid;
      this.offset = offset;
      this.sync = sync;
      this.data = data;
      this.start = start;
      this.length = length;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (offset ^ (offset >>> 32));
      result = prime * result + WritableComparator.hashBytes(data, start, length);
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Write other = (Write) obj;
      if (offset != other.offset)
        return false;
      return Bytes.compareTo(data, start, length, other.data, other.start, other.length) == 0;
    }
    
    
  }
}
