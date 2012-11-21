/**
 * Copyright 2012 Cloudera Inc.
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
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.cloudera.hadoop.hdfs.nfs.metrics.LogMetricPublisher;
import com.cloudera.hadoop.hdfs.nfs.metrics.MetricConstants.Metric;
import com.cloudera.hadoop.hdfs.nfs.metrics.MetricsAccumulator;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.FileHandleINodeMap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSFile;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSStateBackgroundWorker;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCAcceptedException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCException;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCHandler;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.security.AccessPrivilege;
import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;
import com.cloudera.hadoop.hdfs.nfs.security.SecurityHandlerFactory;
import com.cloudera.hadoop.hdfs.nfs.security.SessionSecurityHandler;
import com.cloudera.hadoop.hdfs.nfs.security.Verifier;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
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
  private final SecurityHandlerFactory mSecurityHandlerFactory;
  private final OperationFactory mOperationFactory;
  private final FileSystem mFileSystem;
  private final MetricsAccumulator mMetrics;
  private final AsyncTaskExecutor<CompoundResponse> executor;
  private final HDFSState mHDFSState;
  private final HDFSStateBackgroundWorker mHDFSStateBackgroundWorker;
  private final File[] mTempDirs;

  private final Cache<Key, UserGroupInformation> cache = CacheBuilder.newBuilder().
      expireAfterAccess(30, TimeUnit.MINUTES).removalListener(new RemovalListener<Key, UserGroupInformation>() {
        @Override
        public void onRemoval(RemovalNotification<Key, UserGroupInformation> notification) {
          Key key = notification.getKey();
          UserGroupInformation ugi = notification.getValue();
          try {
            FileSystem.closeAllForUGI(ugi);
          } catch (IOException e) {
            LOGGER.warn("Error closing FileSystem for key = " + 
                key + ", ugi = " + ugi, e);
          }
        }
      }).build();
    
  /**
   * Create a handler with the configuration passed into the constructor
   *
   * @param configuration
   * @throws IOException
   */
  public NFS4Handler(Configuration configuration,
      SecurityHandlerFactory securityHandlerFactory) throws IOException {
    mConfiguration = configuration;
    mSecurityHandlerFactory = securityHandlerFactory;
    mOperationFactory = new OperationFactory();
    mFileSystem = FileSystem.get(mConfiguration);
    mMetrics = new MetricsAccumulator(new LogMetricPublisher(LOGGER),
        TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES));
    String sDataDir = configuration.get(DATA_DIRECTORY);
    if(sDataDir == null) {
      LOGGER.warn("Configuration option " + DATA_DIRECTORY + " is not "
          + "configured, using temp directory to store vital data.");
      sDataDir = Files.createTempDir().getAbsolutePath();
    }
    File dataDir = new File(sDataDir);
    PathUtils.ensureDirectoryIsWriteable(dataDir);
    String[] tempDirs = configuration.getStrings(TEMP_DIRECTORIES);
    if(tempDirs == null) {
      tempDirs = new String[1];
      tempDirs[0] = Files.createTempDir().getAbsolutePath();
    }
    mTempDirs = new File[tempDirs.length];
    for (int i = 0; i < tempDirs.length; i++) {
      mTempDirs[i] = new File(tempDirs[i]);
      PathUtils.fullyDeleteContents(mTempDirs[i]);
      PathUtils.ensureDirectoryIsWriteable(mTempDirs[i]);
    }
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        for(File tempDir : mTempDirs) {
          PathUtils.fullyDelete(tempDir);
        }
      }
    });
    long maxInactiveOpenFileTime = configuration.getInt(MAX_OPEN_FILE_INACTIVITY_PERIOD,
        DEFAULT_MAX_OPEN_FILE_INACTIVITY_PERIOD);
    maxInactiveOpenFileTime = TimeUnit.MILLISECONDS.convert(maxInactiveOpenFileTime, TimeUnit.MINUTES);
    ConcurrentMap<FileHandle, HDFSFile> openFileMap = Maps.newConcurrentMap();
    Map<FileHandle, WriteOrderHandler> writeOrderHandlerMap = Maps.newHashMap();
    File fileHandleINodeDir = new File(dataDir, "fh-to-inode");
    Preconditions.checkState(fileHandleINodeDir.isDirectory() || fileHandleINodeDir.mkdirs());
    FileHandleINodeMap fileHandleINodeMap =
        new FileHandleINodeMap(new File(fileHandleINodeDir, "map"));
    mHDFSState = new HDFSState(fileHandleINodeMap, mTempDirs, mMetrics,
        writeOrderHandlerMap, openFileMap);
    mHDFSStateBackgroundWorker = new HDFSStateBackgroundWorker(mFileSystem, mHDFSState,
        writeOrderHandlerMap, openFileMap, fileHandleINodeMap, 60L * 1000L /* 1 minute*/,
        maxInactiveOpenFileTime, 3L * 24L * 60L * 60L * 1000L /* 3 days */);
    mHDFSStateBackgroundWorker.setDaemon(true);
    mHDFSStateBackgroundWorker.start();
    executor = new AsyncTaskExecutor<CompoundResponse>();
  }
  @Override
  public CompoundRequest createRequest() {
    return new CompoundRequest();
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
  public void incrementMetric(Metric metric, long count) {
    mMetrics.incrementMetric(metric, count);
  }
  
  @Override
  public void beforeProcess(final RPCRequest rpcRequest) throws RPCException {
    if(rpcRequest.getProgram() != NFS_PROG) {
      throw new RPCAcceptedException(RPC_ACCEPT_PROG_UNAVAIL);
    }
    if(rpcRequest.getProgramVersion() != NFS_VERSION) {
      throw new RPCAcceptedException(RPC_ACCEPT_PROG_MISMATCH);
    }
    if(!(rpcRequest.getProcedure() == NFS_PROC_NULL || 
        rpcRequest.getProcedure() == NFS_PROC_COMPOUND)) {
      throw new RPCAcceptedException(RPC_ACCEPT_PROC_UNAVAIL);
    }
  }

  /**
   * Process a CompoundRequest and return a CompoundResponse.
   */
  @Override
  public ListenableFuture<CompoundResponse> process(final RPCRequest rpcRequest,
      final CompoundRequest compoundRequest, AccessPrivilege accessPrivilege,
      final InetAddress clientAddress, final String sessionID) {
    if(compoundRequest.getMinorVersion() != NFS_MINOR_VERSION) {
      CompoundResponse response = new CompoundResponse();
      response.setStatus(NFS4ERR_MINOR_VERS_MISMATCH);
      return Futures.immediateFuture(response);
    }
    AuthenticatedCredentials creds = compoundRequest.getCredentials();
    if (creds == null) {
      CompoundResponse response = new CompoundResponse();
      response.setStatus(NFS4ERR_WRONGSEC);
      return Futures.immediateFuture(response);
    }
    try {
      SessionSecurityHandler<? extends Verifier> securityHandler =
          mSecurityHandlerFactory.getSecurityHandler(creds);
      final String username = securityHandler.getUser();
      Key key = new Key(username, clientAddress.getHostName());
      UserGroupInformation ugi = cache.get(key, new Callable<UserGroupInformation>() {
        @Override
        public UserGroupInformation call() throws Exception {
          if (UserGroupInformation.isSecurityEnabled()) {
            return UserGroupInformation.createProxyUser(username,
                UserGroupInformation.getLoginUser());
          } else {
            return UserGroupInformation.createRemoteUser(username);
          }
        }
      });
      FileSystem fileSystem = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(mConfiguration);
        }
      });
      Session session = new Session(rpcRequest.getXid(), compoundRequest,
          mConfiguration, clientAddress, sessionID, ugi.getShortUserName(), ugi.getGroupNames(),
          fileSystem, accessPrivilege);
      NFS4AsyncFuture task = new NFS4AsyncFuture(mOperationFactory, mHDFSState, session, ugi);
      executor.schedule(task);
      return task;
    } catch (NFS4Exception ex) {
      LOGGER.warn(sessionID, ex);
      CompoundResponse response = new CompoundResponse();
      response.setStatus(ex.getError());
      return Futures.immediateFuture(response);
    } catch (Exception ex) {
      LOGGER.warn(sessionID + " Unhandled Exception", ex);
      CompoundResponse response = new CompoundResponse();
      LOGGER.warn(sessionID + " Setting SERVERFAULT for " + clientAddress
          + " for " + compoundRequest.getOperations());
      response.setStatus(NFS4ERR_SERVERFAULT);
      return Futures.immediateFuture(response);
    }
  }

  public void shutdown() throws IOException {
    mHDFSState.close();
    for(File tempDir : mTempDirs) {
      PathUtils.fullyDelete(tempDir);
    }
  }
  
  private static class Key {
    final String username;
    final String hostname;
    public Key(String username, String hostname) {
      super();
      this.username = username;
      this.hostname = hostname;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
      result = prime * result + ((username == null) ? 0 : username.hashCode());
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
      Key other = (Key) obj;
      if (hostname == null) {
        if (other.hostname != null)
          return false;
      } else if (!hostname.equals(other.hostname))
        return false;
      if (username == null) {
        if (other.username != null)
          return false;
      } else if (!username.equals(other.username))
        return false;
      return true;
    }
    @Override
    public String toString() {
      return "Key [username=" + username + ", hostname=" + hostname + "]";
    }
  }

}
