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

import static com.cloudera.hadoop.hdfs.nfs.PathUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static com.google.common.base.Preconditions.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.OperationRequestHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OperationRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OperationResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCHandler;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.security.Credentials;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsSystem;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * NFS4 Request handler. The class takes a CompoundRequest, processes
 * the request and returns a CompoundResponse.
 */
public class NFS4Handler extends RPCHandler<CompoundRequest, CompoundResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(NFS4Handler.class);
  protected final Map<String, FileHolder> mPathMap = Maps.newConcurrentMap();
  protected final Map<FileHandle, FileHolder> mFileHandleMap = Maps.newConcurrentMap();
  protected final Configuration mConfiguration;
  protected final ClientFactory mClientFactory = new ClientFactory();
  protected static final AtomicLong FILEID = new AtomicLong(0L);
  protected long mStartTime = System.currentTimeMillis();
  protected MetricsPrinter mMetricsPrinter;
  protected Map<String, AtomicLong> mMetrics = Maps.newTreeMap();
  protected Map<FSDataOutputStream, WriteOrderHandler> mWriteOrderHandlerMap = Maps.newConcurrentMap();
  
  /**
   * Create a handler object with a default configuration object
   */
  public NFS4Handler() {
   this(new Configuration()); 
  }
  
  /**
   * Create a handler with the configuration passed into the constructor
   * @param configuration
   */
  public NFS4Handler(Configuration configuration) {
    mConfiguration = configuration;
    mMetricsPrinter = new MetricsPrinter(mMetrics);
    mMetricsPrinter.setName("MetricsPrinter");
    mMetricsPrinter.setDaemon(true);
    mMetricsPrinter.start();
  }
  
  /**
   * Process a CompoundRequest and return a CompoundResponse.
   */
  public CompoundResponse process(final RPCRequest rpcRequest, final CompoundRequest compoundRequest, final String clientHostPort, final String sessionID) {
    Credentials creds = (Credentials)compoundRequest.getCredentials();
    // FIXME below is a hack regarding CredentialsUnix
    if(creds == null || !(creds instanceof CredentialsSystem)) {
      CompoundResponse response = new CompoundResponse();
      response.setStatus(NFS4ERR_WRONGSEC);
      return response;
    }
    try {
      UserGroupInformation sudoUgi;
      String username = creds.getUsername(mConfiguration);
      if (UserGroupInformation.isSecurityEnabled()) {
        sudoUgi = UserGroupInformation.createProxyUser(
          username, UserGroupInformation.getCurrentUser());
      } else {
        sudoUgi = UserGroupInformation.createRemoteUser(username);
      }
      final NFS4Handler server = this;
      final Session session = new Session(rpcRequest.getXid(), compoundRequest, mConfiguration, clientHostPort, sessionID);
      return sudoUgi.doAs(new PrivilegedExceptionAction<CompoundResponse>() {
        public CompoundResponse run() throws Exception {
          String username = UserGroupInformation.getCurrentUser().getShortUserName();
          int lastStatus = NFS4_OK;
          List<OperationResponse> responses = Lists.newArrayList();
          for(OperationRequest request : compoundRequest.getOperations()) {
            if(LOGGER.isDebugEnabled()) {
              LOGGER.debug(sessionID + " " + request.getClass().getSimpleName() + " for " + username);
            }
            OperationRequestHandler<OperationRequest, OperationResponse> handler = 
                OperationFactory.getHandler(request.getID());
            OperationResponse response = handler.handle(server, session, request);
            responses.add(response);
            lastStatus = response.getStatus();
            if(lastStatus != NFS4_OK) {
              LOGGER.warn(sessionID + " Quitting due to " + lastStatus + " on "+ request.getClass().getSimpleName() + " for " + username);
              break;
            }
            server.incrementMetric("NFS_" + request.getClass().getSimpleName(), 1);
            server.incrementMetric("NFS_OPERATIONS", 1);
          }
          CompoundResponse response = new CompoundResponse();
          response.setStatus(lastStatus);
          response.setOperations(responses);
          server.incrementMetric("NFS_COMMANDS", 1);
          return response;          
        }
      });
    } catch (Exception ex) {
      if(ex instanceof UndeclaredThrowableException && ex.getCause() != null) {
        Throwable throwable = ex.getCause();
        if(throwable instanceof Exception) {
          ex = (Exception)throwable;
        } else if(throwable instanceof Error) {
          // something really bad happened
          LOGGER.error(sessionID + " Unhandled Error", throwable);
          throw (Error)throwable;
        } else {
          LOGGER.error(sessionID + " Unhandled Throwable", throwable);
          throw new RuntimeException(throwable);
        }
      }
      LOGGER.warn(sessionID + " Unhandled Exception", ex);
      CompoundResponse response = new CompoundResponse();
      if(ex instanceof NFS4Exception) {
        response.setStatus(((NFS4Exception)ex).getError()); 
      } else if(ex instanceof UnsupportedOperationException) {
        response.setStatus(NFS4ERR_NOTSUPP);
      } else {
        LOGGER.warn(sessionID + " Setting SERVERFAULT for " + clientHostPort + " for " + compoundRequest.getOperations());
        response.setStatus(NFS4ERR_SERVERFAULT);
      }
      return response;
    }
  }
 
  /**
   * Files which are in the process of being created need to 
   * exist from a NFS perspective even if they do not exist
   * from an HDFS perspective. This method intercepts requests
   * for files that are open and calls FileSystem.exists for
   * other files.
   * 
   * @param fs
   * @param path
   * @return true if the file exists or is open for write
   * @throws IOException
   */
  public synchronized boolean fileExists(FileSystem fs, Path path) throws IOException {
    FileHolder fileWrapper = mPathMap.get(realPath(path));
    if(fileWrapper != null && fileWrapper.isOpenForWrite()) {
      return true;
    }
    return fs.exists(path);
  }
  /**
   * Return a FileHandle if it exists or create a new one.
   * 
   * @param path
   * @return a FileHandle
   */
  public synchronized FileHandle createFileHandle(Path path) {
    String realPath = realPath(path);
    FileHolder file = mPathMap.get(realPath);
    if(file != null) {
      return file.getFileHandle();
    }
    String s = UUID.randomUUID().toString().replace("-", "");
    byte[] bytes = s.getBytes();
    FileHandle fileHandle = new FileHandle(bytes);
    FileHolder wrapper = new FileHolder();
    wrapper.setFileHandle(fileHandle);
    wrapper.setPath(realPath);
    mPathMap.put(realPath, wrapper);
    mFileHandleMap.put(fileHandle, wrapper);
    this.incrementMetric("FILE_HANDLES_CREATED", 1);
    return fileHandle;
  }
  /**
   * The NFS fileid is translated to an inode on the host. As
   * such we need to have a unique fileid for each path. Return
   * the file id for a given path or throw a NFSException(STALE).
   * @param path
   * @return fileid
   * @throws NFS4Exception if the file does not have a FileHandle/
   */
  public synchronized long getFileID(Path path) throws NFS4Exception {
    FileHolder file =  mPathMap.get(realPath(path));
    if(file != null) {
      return file.getFileID();
    }
    throw new NFS4Exception(NFS4ERR_STALE, "Path " + realPath(path));
  }
  /**
   * Close the resources allocated in the server, block until the 
   * writes for this file have been processed and close the underlying
   * stream. No lock is held while we wait for the writes to be
   * processed.
   * 
   * @param sessionID
   * @param stateID
   * @param seqID
   * @param fileHandle
   * @return and updated StateID
   * @throws NFS4Exception if the file is open or the stateid is old
   * @throws IOException if the underlying streams throw an excpetion
   */
  public StateID close(String sessionID, StateID stateID, int seqID, FileHandle fileHandle) 
      throws NFS4Exception, IOException {
    FileHolder fileWrapper = null;
    WriteOrderHandler writeOrderHandler = null;
    OpenFile<?> file = null;
    synchronized (this) {
      fileWrapper = mFileHandleMap.get(fileHandle);
      if(fileWrapper != null) {
        if(fileWrapper.isOpenForWrite()) {
          LOGGER.info(sessionID + " Closing " + fileWrapper.getPath() + " for write");
          file = fileWrapper.getFSDataOutputStream();
        } else {
          LOGGER.info(sessionID + " Closing " + fileWrapper.getPath() + " for read");
          file = fileWrapper.getFSDataInputStream(stateID);
        }
        if(file == null) {
          throw new NFS4Exception(NFS4ERR_OLD_STATEID);        
        }
        if(!file.isOwnedBy(stateID)) {
          throw new NFS4Exception(NFS4ERR_FILE_OPEN);
        }
        file.setSequenceID(seqID);
        synchronized (mWriteOrderHandlerMap) {
          writeOrderHandler = mWriteOrderHandlerMap.remove(file.get());
        }
      } else {
        throw new NFS4Exception(NFS4ERR_STALE);
      }
    }
    if(writeOrderHandler != null) {
      writeOrderHandler.close(); // blocks
      LOGGER.info(sessionID + " Closed WriteOrderHandler for " + fileWrapper.getPath());
    }
    synchronized (this) {
      file.close();
      this.incrementMetric("FILES_CLOSED", 1);
      return file.getStateID();
    }
  }
  /**
   * Confirm a file in accordance with NFS OPEN_CONFIRM.
   * 
   * @param stateID
   * @param seqID
   * @param fileHandle
   * @return the stateid associated with the open file
   * @throws NFS4Exception if the stateid is old or open file is owned by another stateid
   */
  public synchronized StateID confirm(StateID stateID, int seqID, FileHandle fileHandle) throws NFS4Exception {
    FileHolder fileWrapper =  mFileHandleMap.get(fileHandle);
    OpenFile<?> file = null;
    if(fileWrapper != null) {
      if(fileWrapper.isOpenForWrite()) {
        file = fileWrapper.getFSDataOutputStream();
      } else {
        file = fileWrapper.getFSDataInputStream(stateID);
      }
      if(file == null) {
        throw new NFS4Exception(NFS4ERR_OLD_STATEID);        
      }
      if(!file.isOwnedBy(stateID)) {
        throw new NFS4Exception(NFS4ERR_FILE_OPEN); // TODO lock unavailable should be _LOCK?
      }
      file.setConfirmed(true);
      file.setSequenceID(seqID);
      return file.getStateID();
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }
  /**
   * Returns true if the file is open.
   * 
   * @param path
   * @return true if the file is open for read or write
   */
  public synchronized boolean isFileOpen(Path path) {
    FileHolder fileWrapper =  mPathMap.get(realPath(path));
    if(fileWrapper != null) {
      return fileWrapper.isOpen();
    }
    return false;
  }
  /**
   * Open if not open or obtain the input stream opened by the StateID.
   *  
   * @param stateID
   * @param fs
   * @param fileHandle
   * @return FSDataInputStream for reading
   * @throws NFS4Exception if the file is already open for write, 
   *  the open is not confirmed or the file handle is stale.
   * @throws IOException if the file open throws an IOException
   */
  public synchronized FSDataInputStream forRead(StateID stateID, FileSystem fs, FileHandle fileHandle) 
      throws NFS4Exception, IOException {
    FileHolder fileWrapper =  mFileHandleMap.get(fileHandle);
    if(fileWrapper != null) {
      if(fileWrapper.isOpenForWrite()) {
        throw new NFS4Exception(NFS4ERR_FILE_OPEN); // TODO lock unavailable should be _LOCK?
      }
      Path path = new Path(fileWrapper.getPath());
      OpenFile<FSDataInputStream> file = fileWrapper.getFSDataInputStream(stateID);
      if(file != null) {
        if(!file.isConfirmed()) {
          throw new NFS4Exception(NFS4ERR_DENIED);
        }
        return file.get();
      }
      FileStatus status = fs.getFileStatus(path);
      if(status.isDir()) {
        throw new NFS4Exception(NFS4ERR_ISDIR);
      }
      FSDataInputStream in = fs.open(path);
      this.incrementMetric("FILES_OPENED_READ", 1);
      fileWrapper.putFSDataInputStream(stateID, in);
      return in;
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }
  /**
   * Create or return the a WriteOrderHandler for a given
   * FSDataOutputStream.
   * 
   * @param name
   * @param out
   * @return the WriteOrderHandler
   * @throws IOException of the output stream throws an IO Exception 
   * while creating the WriteOrderHandler.
   */
  public WriteOrderHandler getWriteOrderHandler(String name, FSDataOutputStream out) throws IOException {
    WriteOrderHandler writeOrderHandler;
    synchronized (mWriteOrderHandlerMap) {
      writeOrderHandler = mWriteOrderHandlerMap.get(out);
      if(writeOrderHandler == null) {
        writeOrderHandler = new WriteOrderHandler(out);
        writeOrderHandler.setDaemon(true);
        writeOrderHandler.setName("WriteOrderHandler-" + name);
        writeOrderHandler.start();
        mWriteOrderHandlerMap.put(out, writeOrderHandler);
      }
    }
    return writeOrderHandler;
  }
  /**
   * Get the WriteOrderHandler for a commit operation.
   * 
   * @param fs
   * @param fileHandle
   * @return WriteOrderHandler
   * @throws NFS4Exception if the file handle is stale
   */
  public synchronized WriteOrderHandler forCommit(FileSystem fs, FileHandle fileHandle) throws NFS4Exception {
    FileHolder fileWrapper =  mFileHandleMap.get(fileHandle);
    if(fileWrapper != null) {
      OpenFile<FSDataOutputStream> file = fileWrapper.getFSDataOutputStream();
      if(file != null) {
        synchronized (mWriteOrderHandlerMap) {
          if(mWriteOrderHandlerMap.containsKey(file.get())) {
            return mWriteOrderHandlerMap.get(file.get());
          }
        }
      }
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }
  
  /**
   * 
   * @param stateID
   * @param fs
   * @param fileHandle
   * @param overwrite
   * @return
   * @throws NFS4Exception
   * @throws IOException
   */
  public synchronized FSDataOutputStream forWrite(StateID stateID, FileSystem fs, FileHandle fileHandle, boolean overwrite) 
      throws NFS4Exception, IOException {
    FileHolder fileWrapper =  mFileHandleMap.get(fileHandle);
    if(fileWrapper != null) {
      OpenFile<FSDataOutputStream> file = fileWrapper.getFSDataOutputStream();
      if(file != null) {
        if(file.isOwnedBy(stateID)) {
          return file.get();          
        }
        throw new NFS4Exception(NFS4ERR_FILE_OPEN);
      }
      Path path = new Path(fileWrapper.getPath());
      boolean exists = fs.exists(path);
      // If overwrite = false, fs.create throws IOException which
      // is useless. In case of IOE do we always return EXIST?
      // doesn't seem to make sense. As such, I am mitigating the issue
      // even if there is a known race between the exists and create
      if(!overwrite && exists) {
        // append to a file
        // We used to be NFS4ERR_EXIST here but the linux client behaved rather oddly.
        // It would open the fily with overwrite=true but then send the data which 
        // was to be appended at offset 0
        throw new NFS4Exception(NFS4ERR_PERM, "File Exists and overwrite = false", true); 
      }
      if(path.getParent() != null) {
        // TODO bad perms will fail with IOException, perhaps we should check
        // that file can be created before trying to so we can return the
        // correct error perm denied        
      }
      if(exists && fs.getFileStatus(path).isDir()) {
        throw new NFS4Exception(NFS4ERR_ISDIR);
      }
      FSDataOutputStream out = fs.create(path, overwrite);
      this.incrementMetric("FILES_OPENED_WRITE", 1);
      fileWrapper.setFSDataOutputStream(stateID, out);
      return out;
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }
  
// TODO use above
//  protected boolean check(String user, List<String> groups, FileStatus status,
//      FsAction access) {
//    FsPermission mode = status.getPermission();
//    if (user.equals(status.getOwner())) { // user class
//      if (mode.getUserAction().implies(access)) {
//        return true;
//      }
//    } else if (groups.contains(status.getGroup())) { // group class
//      if (mode.getGroupAction().implies(access)) {
//        return true;
//      }
//    } else { // other class
//      if (mode.getOtherAction().implies(access)) {
//        return true;
//      }
//    }
//    return false;
//  }
  
  /**
   * Get the Path for a FileHandle
   * @param fileHandle
   * @return Path for FileHandler
   * @throws NFS4Exception if FileHandle is stale
   */
  public Path getPath(FileHandle fileHandle) throws NFS4Exception {
    FileHolder file =  mFileHandleMap.get(fileHandle);
    if(file != null) {
      return new Path(file.getPath());
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }
  /**
   * Get the FileHandle for a Path
   * @param path
   * @return FileHandle for path
   * @throws NFS4Exception if the FileHandle for the Path is stale
   */
  public FileHandle getFileHandle(Path path) throws NFS4Exception {
    FileHolder file = mPathMap.get(realPath(path));
    if(file != null) {
      return file.getFileHandle();        
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }
  /**
   * Files open for write will have an unreliable length according to the name
   * node. As such, this call intercepts calls for open files and returns the
   * length of the as reported by the output stream.
   *  
   * @param status
   * @return the current file length including data written to the output stream
   * @throws NFS4Exception if the getPos() call of the output stream throws IOException
   */
  public long getFileSize(FileStatus status) throws NFS4Exception {
    FileHolder fileWrapper = mPathMap.get(realPath(status.getPath()));
    if(fileWrapper != null) {
      OpenFile<FSDataOutputStream> file = fileWrapper.getFSDataOutputStream();
      if(file != null) {
        try {
          FSDataOutputStream out = file.get();
          return out.getPos();
        } catch (IOException e) {
          throw new NFS4Exception(NFS4ERR_SERVERFAULT, e);
        }          
      }
    }
    return status.getLen();
  }
  /**
   * @return the ClientFactory in use by the NFS4 Handler
   */
  public ClientFactory getClientFactory() {
    return mClientFactory;
  }
  /**
   * Class represents an open input/output stream internally
   * to the NFS4Handler class.
   */
  protected static class OpenFile<T extends Closeable> implements Closeable {
    protected boolean mConfirmed;
    protected T mResource;
    protected long mTimestamp;
    protected StateID mStateID;
    protected FileHolder mFileWrapper;
    protected OpenFile(FileHolder fileWrapper, StateID stateID, T resource) {
      this.mFileWrapper = fileWrapper;
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
      if(mResource != null) {
        if(mResource instanceof InputStream) {
          mFileWrapper.removeInputStream(mStateID);
        } else if(mResource instanceof OutputStream) {
          mFileWrapper.removeOutputStream(mStateID);
        }
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
    public FileHolder getFileHolder() {
      return mFileWrapper;
    }
  }
  /**
   * Class which represents an HDFS file internally
   */
  protected static class FileHolder {
    protected String mPath;
    protected FileHandle mFileHandle;
    protected Map<StateID, OpenFile<FSDataInputStream>> mInputStreams = Maps.newHashMap();
    protected Pair<StateID, OpenFile<FSDataOutputStream>> mOutputStream;
    protected long mFileID = FILEID.addAndGet(10L);
    
    public String getPath() {
      return mPath;
    }
    public void setPath(String path) {
      this.mPath = path;
    }
    public FileHandle getFileHandle() {
      return mFileHandle;
    }
    public void setFileHandle(FileHandle fileHandle) {
      this.mFileHandle = fileHandle;
    }
    public OpenFile<FSDataInputStream> getFSDataInputStream(StateID stateID) {
      if(mInputStreams.containsKey(stateID)) {
        OpenFile<FSDataInputStream> file = mInputStreams.get(stateID);
        file.setTimestamp(System.currentTimeMillis());
        return file;        
      }
      return null;
    }
    public void putFSDataInputStream(StateID stateID, FSDataInputStream fsDataInputStream) {
      mInputStreams.put(stateID, new OpenFile<FSDataInputStream>(this, stateID, fsDataInputStream));
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
    public OpenFile<FSDataOutputStream> getFSDataOutputStream() {
      if(mOutputStream != null) {
        OpenFile<FSDataOutputStream> file = mOutputStream.getSecond();
        file.setTimestamp(System.currentTimeMillis());
        return file;
      }
      return null;
    }
    public OpenFile<FSDataInputStream> removeInputStream(StateID stateID) {
      return mInputStreams.remove(stateID);
    }
    public void removeOutputStream(StateID stateID) {
      if(mOutputStream != null) {
        checkState(stateID.equals(mOutputStream.getFirst()), "stateID " + stateID + " does not own file");
      }
      mOutputStream = null;
    }
    public void setFSDataOutputStream(StateID stateID, FSDataOutputStream fsDataOutputStream) {
      mOutputStream = Pair.of(stateID, new OpenFile<FSDataOutputStream>(this, stateID, fsDataOutputStream));
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
          + ", mFSDataOutputStream=" + mOutputStream + ", mFileID="
          + mFileID + "]";
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      FileHolder other = (FileHolder) obj;
      if (mFileHandle == null) {
        if (other.mFileHandle != null)
          return false;
      } else if (!mFileHandle.equals(other.mFileHandle))
        return false;
      if (mFileID != other.mFileID)
        return false;
      if (mPath == null) {
        if (other.mPath != null)
          return false;
      } else if (!mPath.equals(other.mPath))
        return false;
      return true;
    }
  }

  /**
   * Create or increment a named counter
   */
  public void incrementMetric(String name, long count) {
    name = name.toUpperCase();
    AtomicLong counter;
    synchronized(mMetrics) {
      counter  = mMetrics.get(name);
      if(counter == null) {
        counter = new AtomicLong(0);
        mMetrics.put(name, counter);
      }
    }
    counter.addAndGet(count);
  }

  /**
   * Simple thread used to dump out metrics to 
   * the log every minute so. FIXME use hadoop metrics
   */
  protected static class MetricsPrinter extends Thread {
    Map<String, AtomicLong> mMetrics;
    public MetricsPrinter( Map<String, AtomicLong> metrics) {
      mMetrics = metrics;
    }
    public void run() {
      try {
        while(true) {
          Thread.sleep(60000L);
          synchronized(mMetrics) {
            LOGGER.info("Metrics Start");
            for(String key : mMetrics.keySet()) {
              AtomicLong counter = mMetrics.get(key);
              long value = counter.getAndSet(0);
              if(key.contains("_BYTES_")) {
                LOGGER.info("Metric: " + key + " = " + Bytes.toHuman(value/60L) + "/sec");                
              } else {
                LOGGER.info("Metric: " + key + " = " + value);
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Metrics Printer Quitting", e);
      }
    }
  }
  
  @Override
  public CompoundResponse createResponse() {
    return new CompoundResponse();
  }
  
  @Override
  public CompoundRequest createRequest() {
    return new CompoundRequest();
  }
  /**
   * @return start time of the NFS server
   */
  public long getStartTime() {
    return mStartTime;
  }
}
