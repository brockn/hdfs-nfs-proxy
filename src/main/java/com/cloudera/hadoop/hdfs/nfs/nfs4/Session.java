/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.hadoop.hdfs.nfs.nfs4;


import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;

/**
 * Class represents the state of a request as
 * we process each individual request in the
 * compound request
 */
public class Session {
  protected FileHandle mCurrentFileHandle;
  protected FileHandle mSavedFileHandle;
  protected final Configuration mConfiguration;
  protected final CompoundRequest mCompoundRequest;
  protected final FileSystem mFileSystem;
  protected final InetAddress mClientAddress;
  protected final String mSessionID;
  protected final int mXID;
  public Session(int xid, CompoundRequest compoundRequest, Configuration configuration, InetAddress clientAddress, String sessionID)
      throws IOException {
    mXID = xid;
    mCompoundRequest = compoundRequest;
    mConfiguration = configuration;
    mFileSystem = FileSystem.get(mConfiguration);
    mClientAddress = clientAddress;
    mSessionID = sessionID;
  }
  public int getXID() {
    return mXID;
  }
  public FileHandle getCurrentFileHandle() {
    return mCurrentFileHandle;
  }

  public void setCurrentFileHandle(FileHandle currentFileHandle) {
    this.mCurrentFileHandle = currentFileHandle;
  }

  public FileHandle getSavedFileHandle() {
    return mSavedFileHandle;
  }

  public void setSavedFileHandle(FileHandle savedFileHandle) {
    this.mSavedFileHandle = savedFileHandle;
  }
  public Configuration getConfiguration() {
    return mConfiguration;
  }
  public CompoundRequest getCompoundRequest() {
    return mCompoundRequest;
  }
  public FileSystem getFileSystem() {
    return mFileSystem;
  }
  public InetAddress getClientAddress() {
    return mClientAddress;
  }
  public String getSessionID() {
    return mSessionID;
  }
}
