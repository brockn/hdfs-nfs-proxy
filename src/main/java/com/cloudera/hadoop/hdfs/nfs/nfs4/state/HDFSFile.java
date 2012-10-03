package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import static com.google.common.base.Preconditions.checkState;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.google.common.collect.Maps;

/**
 * Class which represents an HDFS file internally
 */
public class HDFSFile {


  protected final FileHandle mFileHandle;
  protected final String mPath;
  protected final long mFileID;
  protected final Map<StateID, OpenResource<FSDataInputStream>> mInputStreams = Maps.newHashMap();
  protected Pair<StateID, OpenResource<FSDataOutputStream>> mOutputStream;

  public HDFSFile(FileHandle fileHandle, String path, long fileID) {
    this.mFileHandle = fileHandle;
    this.mPath = path;
    this.mFileID = fileID;
  }

  public String getPath() {
    return mPath;
  }

  public FileHandle getFileHandle() {
    return mFileHandle;
  }

  public OpenResource<FSDataInputStream> getFSDataInputStream(StateID stateID) {
    if (mInputStreams.containsKey(stateID)) {
      OpenResource<FSDataInputStream> file = mInputStreams.get(stateID);
      file.setTimestamp(System.currentTimeMillis());
      return file;
    }
    return null;
  }

  public void putFSDataInputStream(StateID stateID,
      FSDataInputStream fsDataInputStream) {
    mInputStreams.put(stateID, new OpenResource<FSDataInputStream>(this, stateID,
        fsDataInputStream));
  }

  public boolean isOpen() {
    return isOpenForWrite() || isOpenForRead();
  }

  public boolean isOpenForRead() {
    return !mInputStreams.isEmpty();
  }

  public boolean isOpenForWrite() {
    return getFSDataOutputStream() != null;
  }

  public OpenResource<FSDataOutputStream> getFSDataOutputStream() {
    if (mOutputStream != null) {
      OpenResource<FSDataOutputStream> file = mOutputStream.getSecond();
      file.setTimestamp(System.currentTimeMillis());
      return file;
    }
    return null;
  }

  public void removeResource(Object resource, StateID stateID) {
    if (resource instanceof InputStream) {
      mInputStreams.remove(stateID);
    } else if (resource instanceof OutputStream) {
      if (mOutputStream != null) {
        checkState(stateID.equals(mOutputStream.getFirst()), "stateID "
            + stateID + " does not own file");
      }
      mOutputStream = null;
    }
  }

  public void setFSDataOutputStream(StateID stateID,
      FSDataOutputStream fsDataOutputStream) {
    mOutputStream = Pair.of(stateID, new OpenResource<FSDataOutputStream>(this,
        stateID, fsDataOutputStream));
  }

  public long getFileID() {
    return mFileID;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((mFileHandle == null) ? 0 : mFileHandle.hashCode());
    result = prime * result + (int) (mFileID ^ (mFileID >>> 32));
    result = prime * result + ((mPath == null) ? 0 : mPath.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "FileHolder [mPath=" + mPath + ", mFileHandle=" + mFileHandle
        + ", mFSDataOutputStream=" + mOutputStream + ", mFileID=" + mFileID
        + "]";
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
    HDFSFile other = (HDFSFile) obj;
    if (mFileHandle == null) {
      if (other.mFileHandle != null) {
        return false;
      }
    } else if (!mFileHandle.equals(other.mFileHandle)) {
      return false;
    }
    if (mFileID != other.mFileID) {
      return false;
    }
    if (mPath == null) {
      if (other.mPath != null) {
        return false;
      }
    } else if (!mPath.equals(other.mPath)) {
      return false;
    }
    return true;
  }
}