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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;

import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Callback;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FixedUserIDMapper;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.Client;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.ClientFactory;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.cloudera.hadoop.hdfs.nfs.security.AccessPrivilege;
import com.google.common.base.Charsets;

public class TestBaseHandler {

  protected HDFSState hdfsState;
  protected Session session;
  protected FileSystem fs;
  protected FileHandle currentFileHandle = new FileHandle("current".getBytes(Charsets.UTF_8));
  protected FileHandle savedFileHandle = new FileHandle("saved".getBytes());
  protected FileStatus fileStatus;
  protected FsPermission filePermissions;
  protected FileStatus notdir, isdir;
  protected Configuration configuration;
  protected ClientFactory clientFactory;
  protected String hostname;
  protected Client client;
  protected Callback callback;
  protected ClientID clientID;
  protected OpaqueData8 clientVerifier;

  @Before
  public void setup() throws Exception {
    hostname = "localhost";
    clientVerifier = new OpaqueData8();
    hdfsState = mock(HDFSState.class);
    session = mock(Session.class);
    fs = mock(FileSystem.class);
    fileStatus = mock(FileStatus.class);
    filePermissions = mock(FsPermission.class);
    configuration = new Configuration();
    clientFactory = mock(ClientFactory.class);
    InetAddress addr = mock(InetAddress.class);
    client = mock(Client.class);
    callback = mock(Callback.class);
    clientID = mock(ClientID.class);

    when(client.getVerifer()).thenReturn(clientVerifier);
    when(client.getClientID()).thenReturn(clientID);
    when(client.getClientHost()).thenReturn(hostname);
    when(clientFactory.get(any(OpaqueData.class))).thenReturn(client);
    when(clientFactory.getByShortHand(any(Long.class))).thenReturn(client);
    when(hdfsState.getClientFactory()).thenReturn(clientFactory);
    when(addr.getCanonicalHostName()).thenReturn(hostname);

    when(fileStatus.getPermission()).thenReturn(filePermissions);
    when(fs.getFileStatus(any(Path.class))).thenReturn(fileStatus);
    when(session.getFileSystem()).thenReturn(fs);
    when(session.getAccessPrivilege()).thenReturn(AccessPrivilege.READ_WRITE);
    when(session.getClientAddress()).thenReturn(addr);
    when(hdfsState.getPath(currentFileHandle)).thenReturn(new Path("/"));
    when(session.getCurrentFileHandle()).thenReturn(currentFileHandle);
    when(session.getConfiguration()).thenReturn(configuration);
    when(session.getUser()).thenReturn(FixedUserIDMapper.getCurrentUser());
    when(session.getGroups()).thenReturn(new String[] { FixedUserIDMapper.getCurrentGroup()});
    notdir = mock(FileStatus.class);
    when(notdir.isDir()).thenReturn(false);

    isdir = mock(FileStatus.class);
    when(isdir.isDir()).thenReturn(true);
    
    CompoundRequest compoundRequest = new CompoundRequest();
    when(session.getCompoundRequest()).thenReturn(compoundRequest);
    compoundRequest.setCredentials(TestUtils.newCredentials());

  }

}
