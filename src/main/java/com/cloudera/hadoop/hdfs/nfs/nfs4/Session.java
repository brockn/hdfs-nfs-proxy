/**
 * Copyright 2012 Cloudera Inc.
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
import com.cloudera.hadoop.hdfs.nfs.security.AccessPrivilege;

/**
 * Class represents the state of a request as
 * we process each individual request in the
 * compound request
 */
public class Session {
  private FileHandle mCurrentFileHandle;
  private FileHandle mSavedFileHandle;
  private final Configuration mConfiguration;
  private final CompoundRequest mCompoundRequest;
  private final InetAddress mClientAddress;
  private final String mSessionID;
  private final int mXID;
  private final String mUser;
  private final String[] mGroups;
  private final FileSystem mFileSystem;
  private final AccessPrivilege mAccessPrivilege;
  
  public Session(int xid, CompoundRequest compoundRequest, Configuration configuration,
      InetAddress clientAddress, String sessionID, String user, String[] groups, FileSystem fs,
      AccessPrivilege accessPrivilege)
      throws IOException {
    mXID = xid;
    mCompoundRequest = compoundRequest;
    mConfiguration = configuration;
    mClientAddress = clientAddress;
    mSessionID = sessionID;
    mUser = user;
    mGroups = groups;
    mFileSystem = fs;
    mAccessPrivilege = accessPrivilege;
  }
  public InetAddress getClientAddress() {
    return mClientAddress;
  }
  public CompoundRequest getCompoundRequest() {
    return mCompoundRequest;
  }
  public Configuration getConfiguration() {
    return mConfiguration;
  }
  public FileHandle getCurrentFileHandle() {
    return mCurrentFileHandle;
  }
  public FileSystem getFileSystem() {
    return mFileSystem;
  }
  public AccessPrivilege getAccessPrivilege() {
    return mAccessPrivilege;
  }
  public String[] getGroups() {
    return mGroups;
  }
  public String getUser() {
    return mUser;
  }
  public FileHandle getSavedFileHandle() {
    return mSavedFileHandle;
  }
  public String getSessionID() {
    return mSessionID;
  }
  public int getXID() {
    return mXID;
  }
  public String getXIDAsHexString() {
    return Integer.toHexString(mXID);
  }
  public void setCurrentFileHandle(FileHandle currentFileHandle) {
    this.mCurrentFileHandle = currentFileHandle;
  }
  public void setSavedFileHandle(FileHandle savedFileHandle) {
    this.mSavedFileHandle = savedFileHandle;
  }
  @Override
  public String toString() {
    return "Session [mCurrentFileHandle=" + mCurrentFileHandle
        + ", mSavedFileHandle=" + mSavedFileHandle + ", mCompoundRequest="
        + mCompoundRequest + ", mClientAddress=" + mClientAddress
        + ", mSessionID=" + mSessionID + ", mXID=" + getXIDAsHexString() 
        + ", mAccessPrivilege = " + mAccessPrivilege + "]";
  }
  
}
