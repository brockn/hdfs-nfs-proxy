/**
 * Copyright 2012 The Apache Software Foundation
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

/**
 * Exception representing an NFS specific error code.
 */
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
