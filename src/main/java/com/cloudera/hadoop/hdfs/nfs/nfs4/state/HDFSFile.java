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
package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import static com.google.common.base.Preconditions.checkState;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;

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
  protected Pair<StateID, OpenResource<HDFSOutputStream>> mOutputStream;

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
    return getHDFSOutputStream() != null;
  }

  public OpenResource<HDFSOutputStream> getHDFSOutputStream() {
    if (mOutputStream != null) {
      OpenResource<HDFSOutputStream> file = mOutputStream.getSecond();
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

  public void setHDFSOutputStream(StateID stateID,
      HDFSOutputStream fsDataOutputStream) {
    mOutputStream = Pair.of(stateID, new OpenResource<HDFSOutputStream>(this,
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