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


abstract class AbstractPendingWrite implements PendingWrite {

  private final String name;
  private final int xid;
  private final long offset;
  private final boolean sync;

  public AbstractPendingWrite(String name, int xid, long offset, boolean sync) {
    this.name = name;
    this.xid = xid;
    this.offset = offset;
    this.sync = sync;
  }
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getXidAsHexString() {
    return Integer.toHexString(xid);
  }
  @Override
  public int getXid() {
    return xid;
  }
  @Override
  public long getOffset() {
    return offset;
  }
  @Override
  public boolean isSync() {
    return sync;
  }
  @Override
  public void close() {
    
  }
  protected static int getHashCode(long offset, byte[] data, int start, int length) {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (offset ^ (offset >>> 32));
    result = prime * result + hashBytes(data, start, length);
    return result;
  }
  private static int hashBytes(byte[] bytes, int offset, int length) {
    int hash = 1;
    for (int i = offset; i < offset + length; i++) {
      hash = (31 * hash) + bytes[i];
    }
    return hash;
  }

}