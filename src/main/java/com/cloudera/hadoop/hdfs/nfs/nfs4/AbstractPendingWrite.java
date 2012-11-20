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

import com.cloudera.hadoop.hdfs.nfs.Bytes;


abstract class AbstractPendingWrite implements PendingWrite {

  protected static int getHashCode(long offset, byte[] data, int start, int length) {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + (int) (offset ^ (offset >>> 32));
    result = (prime * result) + Bytes.hashBytes(data, start, length);
    return result;
  }
  private final int xid;
  private final long offset;
  private final boolean sync;

  public AbstractPendingWrite(int xid, long offset, boolean sync) {
    this.xid = xid;
    this.offset = offset;
    this.sync = sync;
  }
  @Override
  public long getOffset() {
    return offset;
  }
  @Override
  public int getXid() {
    return xid;
  }
  @Override
  public String getXidAsHexString() {
    return Integer.toHexString(xid);
  }
  @Override
  public boolean isSync() {
    return sync;
  }

}