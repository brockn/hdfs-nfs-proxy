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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.cloudera.hadoop.hdfs.nfs.metrics.LogMetricPublisher;
import com.cloudera.hadoop.hdfs.nfs.metrics.MetricConstants.Metric;
import com.cloudera.hadoop.hdfs.nfs.metrics.MetricsAccumulator;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSFile;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSStateBackgroundWorker;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCHandler;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;
import com.cloudera.hadoop.hdfs.nfs.security.Credentials;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * NFS4 Request handler. The class takes a CompoundRequest, processes the
 * request and returns a CompoundResponse.
 */
public class NFS4Handler extends RPCHandler<CompoundRequest, CompoundResponse> {
  protected static final Logger LOGGER = Logger.getLogger(NFS4Handler.class);
  private final Configuration mConfiguration;
  private final MetricsAccumulator mMetrics;
  private final AsyncTaskExecutor<CompoundResponse> executor;
  private final HDFSState mHDFSState;
  private final HDFSStateBackgroundWorker mHDFSStateBackgroundWorker;
  private final String[] mTempDirs;
  
  /**
   * Create a handler object with a default configuration object
   * @throws IOException 
   */
  public NFS4Handler() throws IOException {
    this(new Configuration());
  }

  /**
   * Create a handler with the configuration passed into the constructor
   *
   * @param configuration
   * @throws IOException 
   */
  public NFS4Handler(Configuration configuration) throws IOException {
    mConfiguration = configuration;
    mMetrics = new MetricsAccumulator(new LogMetricPublisher(LOGGER), 
        TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    mTempDirs = configuration.getStrings(TEMP_DIRECTORIES, Files.createTempDir().getAbsolutePath());
    FileHandleStore fileHandleStore = FileHandleStore.get(configuration);    
    long maxInactiveOpenFileTime = configuration.getInt(MAX_OPEN_FILE_INACTIVITY_PERIOD, 
        DEFAULT_MAX_OPEN_FILE_INACTIVITY_PERIOD);
    maxInactiveOpenFileTime = TimeUnit.MILLISECONDS.convert(maxInactiveOpenFileTime, TimeUnit.MINUTES);
    ConcurrentMap<FileHandle, HDFSFile> fileHandleMap = Maps.newConcurrentMap();
    Map<FileHandle, WriteOrderHandler> writeOrderHandlerMap = Maps.newHashMap();
    mHDFSState = new HDFSState(fileHandleStore, mTempDirs, mMetrics, writeOrderHandlerMap, fileHandleMap);
    mHDFSStateBackgroundWorker = new HDFSStateBackgroundWorker(writeOrderHandlerMap, 
        fileHandleMap, 60L * 1000L, maxInactiveOpenFileTime);
    mHDFSStateBackgroundWorker.setDaemon(true);
    mHDFSStateBackgroundWorker.start();
    executor = new AsyncTaskExecutor<CompoundResponse>();
  }
  public void shutdown() throws IOException {
    mHDFSState.close();
    for(String tempDir : mTempDirs) {
      try {
        PathUtils.fullyDelete(new File(tempDir));
      } catch (IOException e) {
        LOGGER.warn("Error deleting " + tempDir, e);
      }
    }
  }

  /**
   * Process a CompoundRequest and return a CompoundResponse.
   */
  @Override
  public ListenableFuture<CompoundResponse> process(final RPCRequest rpcRequest,
      final CompoundRequest compoundRequest, final InetAddress clientAddress,
      final String sessionID) {
    Credentials creds = (Credentials) compoundRequest.getCredentials();
    // FIXME below is a hack regarding CredentialsUnix
    if (creds == null || !(creds instanceof AuthenticatedCredentials)) {
      CompoundResponse response = new CompoundResponse();
      response.setStatus(NFS4ERR_WRONGSEC);
      return Futures.immediateFuture(response);
    }
    try {
      UserGroupInformation sudoUgi;
      String username = creds.getUsername(mConfiguration);
      if (UserGroupInformation.isSecurityEnabled()) {
        sudoUgi = UserGroupInformation.createProxyUser(username,
            UserGroupInformation.getCurrentUser());
      } else {
        sudoUgi = UserGroupInformation.createRemoteUser(username);
      }
      Session session = new Session(rpcRequest.getXid(), compoundRequest,
          mConfiguration, clientAddress, sessionID);
      NFS4AsyncFuture task = new NFS4AsyncFuture(mHDFSState, session, sudoUgi);
      executor.schedule(task);
      return task;
    } catch (Exception ex) {
      LOGGER.warn(sessionID + " Unhandled Exception", ex);
      CompoundResponse response = new CompoundResponse();
      LOGGER.warn(sessionID + " Setting SERVERFAULT for " + clientAddress
          + " for " + compoundRequest.getOperations());
      response.setStatus(NFS4ERR_SERVERFAULT);
      return Futures.immediateFuture(response);
    }
  }


  @Override
  public void incrementMetric(Metric metric, long count) {
    mMetrics.incrementMetric(metric, count);
  }

  /**
   * Simple thread used to dump out metrics to the log every minute so. FIXME
   * use hadoop metrics
   */
  

  @Override
  public CompoundResponse createResponse() {
    return new CompoundResponse();
  }

  @Override
  public CompoundRequest createRequest() {
    return new CompoundRequest();
  }
}
