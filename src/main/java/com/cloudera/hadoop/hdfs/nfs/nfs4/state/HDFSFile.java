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

  private final FileHandle mFileHandle;
  private final String mPath;
  private final long mFileID;
  private final Map<StateID, OpenResource<FSDataInputStream>> mInputStreams = Maps.newHashMap();
  private Pair<StateID, OpenResource<HDFSOutputStream>> mOutputStream;

  public HDFSFile(FileHandle fileHandle, String path, long fileID) {
    this.mFileHandle = fileHandle;
    this.mPath = path;
    this.mFileID = fileID;
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
    mInputStreams.put(stateID, new OpenResource<FSDataInputStream>(stateID,
        fsDataInputStream));
  }

  public boolean isOpen() {
    return isOpenForWrite() || isOpenForRead();
  }

  public boolean isOpenForRead() {
    return !mInputStreams.isEmpty();
  }

  public boolean isOpenForWrite() {
    return getHDFSOutputStreamForWrite() != null;
  }
  public OpenResource<HDFSOutputStream> getHDFSOutputStream() {
    if (mOutputStream != null) {
      return mOutputStream.getSecond();
    }
    return null;
  }
  public OpenResource<HDFSOutputStream> getHDFSOutputStreamForWrite() {
    if (mOutputStream != null) {
      OpenResource<HDFSOutputStream> file = mOutputStream.getSecond();
      file.setTimestamp(System.currentTimeMillis());
      return file;
    }
    return null;
  }

  public void closeInputStream(StateID stateID) throws IOException {
    OpenResource<?> resource = mInputStreams.remove(stateID);
    if(resource != null) {
      resource.close();
    }
  }
  public void closeOutputStream(StateID stateID) throws IOException {
    if(mOutputStream != null && mOutputStream.getFirst().equals(stateID)) {
      mOutputStream.getSecond().close();
      mOutputStream = null;
    }
  }
  public void setHDFSOutputStream(StateID stateID,
      HDFSOutputStream fsDataOutputStream) {
    mOutputStream = Pair.of(stateID, new OpenResource<HDFSOutputStream>(stateID, fsDataOutputStream));
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
  public String toString() {
    return "HDFSFile [mPath=" + mPath + ", mFileHandle=" + mFileHandle
        + ", Pair<StateID, HDFSOutputStream> =" + mOutputStream + ", mFileID=" + mFileID
        + "]";
  }
}