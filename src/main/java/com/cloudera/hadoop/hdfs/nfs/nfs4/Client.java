package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.util.concurrent.atomic.AtomicLong;

public class Client {
  
  protected static final AtomicLong CLIENTID = new AtomicLong(0L);

  
  protected ClientID mClientID;
  protected Callback mCallback;
  protected int mCallbackIdent;
  protected long mShorthandID;
  protected OpaqueData8 mVerifer;
  protected String mClientHost;
  protected String mClientHostPort;
  protected boolean mConfirmed;
  protected long mRenew = System.currentTimeMillis();
  
  public boolean isConfirmed() {
    return mConfirmed;
  }

  public void setConfirmed(boolean confirmed) {
    this.mConfirmed = confirmed;
  }

  protected Client(ClientID clientID) {
    this.mClientID = clientID;
    mShorthandID = CLIENTID.addAndGet(10L);
  }

  public ClientID getClientID() {
    return mClientID;
  }

  public void setClientID(ClientID clientID) {
    this.mClientID = clientID;
  }

  public Callback getCallback() {
    return mCallback;
  }

  public void setCallback(Callback callback) {
    this.mCallback = callback;
  }

  public int getCallbackIdent() {
    return mCallbackIdent;
  }

  public void setCallbackIdent(int callbackIdent) {
    this.mCallbackIdent = callbackIdent;
  }

  public long getShorthandID() {
    return mShorthandID;
  }

  public void setShorthandID(long shorthandID) {
    this.mShorthandID = shorthandID;
  }
  public void setClientHost(String host) {
    this.mClientHost = host;
  }
  public String getClientHost() {
    return mClientHost;
  }
  public void setClientHostPort(String hostPort) {
    this.mClientHostPort = hostPort;
  }
  public String getClientHostPort() {
    return mClientHostPort;
  }
  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }
  public void setRenew(long ts) {
    mRenew = ts;
  }
}
