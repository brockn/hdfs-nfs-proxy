package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import java.io.Closeable;
import java.io.IOException;

import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;

/**
 * Class represents an open input/output stream internally to the
 * NFS4Handler class.
 */
public class OpenResource<T extends Closeable> implements Closeable {

  private final HDFSFile mHDFSFile;
  private final T mResource;
  private final StateID mStateID;
  private boolean mConfirmed;
  private long mTimestamp;

  public OpenResource(HDFSFile hdfsFile, StateID stateID, T resource) {
    this.mHDFSFile = hdfsFile;
    this.mStateID = stateID;
    this.mResource = resource;
    mTimestamp = System.currentTimeMillis();
  }

  public void setSequenceID(int seqID) {
    mStateID.setSeqID(seqID);
  }

  public boolean isOwnedBy(StateID stateID) {
    return mStateID.equals(stateID);
  }

  public T get() {
    return mResource;
  }

  @Override
  public void close() throws IOException {
    if (mResource != null) {
      mHDFSFile.removeResource(mResource, mStateID);
      synchronized (mResource) {
        mResource.close();
      }
    }
  }

  public boolean isConfirmed() {
    return mConfirmed;
  }

  public void setConfirmed(boolean confirmed) {
    mConfirmed = confirmed;
  }

  public long getTimestamp() {
    return mTimestamp;
  }

  public void setTimestamp(long timestamp) {
    this.mTimestamp = timestamp;
  }

  public StateID getStateID() {
    return mStateID;
  }

  public HDFSFile getHDFSFile() {
    return mHDFSFile;
  }
}