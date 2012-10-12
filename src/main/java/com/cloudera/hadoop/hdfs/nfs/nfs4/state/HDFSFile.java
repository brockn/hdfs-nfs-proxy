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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.google.common.collect.Maps;

/**
 * Class which represents an HDFS file internally
 */
public class HDFSFile {

  private final FileHandle mFileHandle;
  private final String mPath;
  private final long mFileID;
  private final Map<StateID, OpenResource<HDFSInputStream>> mInputStreams = Maps.newHashMap();
  private Pair<StateID, OpenResource<HDFSOutputStream>> mOutputStream;

  public HDFSFile(FileHandle fileHandle, String path, long fileID) {
    this.mFileHandle = fileHandle;
    this.mPath = path;
    this.mFileID = fileID;
  }

  public synchronized OpenResource<HDFSInputStream> getInputStream(StateID stateID) {
    if (mInputStreams.containsKey(stateID)) {
      OpenResource<HDFSInputStream> file = mInputStreams.get(stateID);
      file.setTimestamp(System.currentTimeMillis());
      return file;
    }
    return null;
  }

  public synchronized void putInputStream(StateID stateID,
      HDFSInputStream inputStream) {
    mInputStreams.put(stateID, new OpenResource<HDFSInputStream>(stateID,
        inputStream));
  }

  public synchronized boolean isOpen() {
    return isOpenForWrite() || isOpenForRead();
  }

  public synchronized boolean isOpenForRead() {
    return !mInputStreams.isEmpty();
  }

  public synchronized boolean isOpenForWrite() {
    return getHDFSOutputStreamForWrite() != null;
  }
  public synchronized OpenResource<HDFSOutputStream> getHDFSOutputStream() {
    if (mOutputStream != null) {
      return mOutputStream.getSecond();
    }
    return null;
  }
  public synchronized OpenResource<HDFSOutputStream> getHDFSOutputStreamForWrite() {
    if (mOutputStream != null) {
      OpenResource<HDFSOutputStream> file = mOutputStream.getSecond();
      file.setTimestamp(System.currentTimeMillis());
      return file;
    }
    return null;
  }

  public synchronized void closeResourcesInactiveSince(long inactiveSince) 
      throws IOException {
    if (mOutputStream != null) {
      OpenResource<?> resource = mOutputStream.getSecond();
      if(resource.getTimestamp() < inactiveSince) {
        mOutputStream = null;
        resource.close();
      }
    }
    Set<StateID> keys = new HashSet<StateID>(mInputStreams.keySet());
    for(StateID stateID : keys) {
      OpenResource<?> resource = mInputStreams.get(stateID);
      if(resource.getTimestamp() < inactiveSince) {
        mInputStreams.remove(stateID);
        resource.close();
      }
    }
  }
  public synchronized void closeInputStream(StateID stateID) throws IOException {
    OpenResource<?> resource = mInputStreams.remove(stateID);
    if(resource != null) {
      resource.close();
    }
  }
  public synchronized void closeOutputStream(StateID stateID) throws IOException {
    if(mOutputStream != null) {
      OpenResource<HDFSOutputStream> res = mOutputStream.getSecond();
      if(stateID.equals(mOutputStream.getFirst())) {
        mOutputStream = null;
        res.close();
      }
    }
  }
  public synchronized void setHDFSOutputStream(StateID stateID, HDFSOutputStream outputStream) {
    mOutputStream = Pair.of(stateID, new OpenResource<HDFSOutputStream>(stateID, outputStream));
  }
  public String getPath() {
    return mPath;
  }

  public FileHandle getFileHandle() {
    return mFileHandle;
  }
  public long getFileID() {
    return mFileID;
  }

  @Override
  public synchronized String toString() {
    return "HDFSFile [mPath=" + mPath + ", mFileHandle=" + mFileHandle
        + ", Pair<StateID, HDFSOutputStream> =" + mOutputStream + ", mFileID=" + mFileID
        + "]";
  }
}