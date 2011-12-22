package com.cloudera.hadoop.hdfs.nfs.nfs4;


import java.io.IOException;

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
  protected final String mClientHostPort, mClientHost;
  protected final String mSessionID;
  protected final int mXID;
  public Session(int xid, CompoundRequest compoundRequest, Configuration configuration, String clientHostPort, String sessionID) 
      throws IOException {
    mXID = xid;
    mCompoundRequest = compoundRequest;
    mConfiguration = configuration;
    mFileSystem = FileSystem.get(mConfiguration);
    mClientHostPort = clientHostPort;
    int pos;
    if((pos = mClientHostPort.indexOf(":")) > 0) {
      mClientHost = mClientHostPort.substring(0, pos);
    } else {
      mClientHost = mClientHostPort;
    }
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
  public String getClientHost() {
    return mClientHost;
  }
  public String getClientHostPort() {
    return mClientHostPort;
  }
  public String getSessionID() {
    return mSessionID;
  }
}
