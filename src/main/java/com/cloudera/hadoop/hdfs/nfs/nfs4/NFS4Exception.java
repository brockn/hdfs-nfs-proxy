package com.cloudera.hadoop.hdfs.nfs.nfs4;

public class NFS4Exception extends Exception {
  private static final long serialVersionUID = 6096916929311308879L;
  int error;
  boolean log;
  public NFS4Exception(int error) {
    this(error, (String)null);
  }
  public NFS4Exception(int error, String msg) {
    this(error, msg, false);
  }
  public NFS4Exception(int error, String msg, boolean log) {
    super(msg);
    this.error = error;
    this.log = log;
  }
  public NFS4Exception(int error, Exception e) {
    super(e);
    this.error = error;
  }
  public NFS4Exception(int error, String msg, Exception e) {
    super(msg, e);
    this.error = error;
  }
  public int getError() {
    return error;
  }
  public boolean shouldLog() {
    return log;
  }
  @Override
  public String getMessage() {
    String msg = "errno = " + error;
    if(super.getMessage() != null) {
      msg += ": " + super.getMessage();      
    }
    return msg;
  }
}
