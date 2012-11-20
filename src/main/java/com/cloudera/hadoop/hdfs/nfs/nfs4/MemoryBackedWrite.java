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

public class MemoryBackedWrite extends AbstractPendingWrite {
  private final byte[] data;
  private final int start;
  private final int length;
  private final int hashCode;
  public MemoryBackedWrite(int xid, long offset, boolean sync,
      byte[] data, int start, int length) {
    super(xid, offset, sync);
    this.data = data;
    this.start = start;
    this.length = length;
    this.hashCode = getHashCode(offset, data, start, length);
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MemoryBackedWrite other = (MemoryBackedWrite) obj;
    if (getOffset() != other.getOffset()) {
      return false;
    }
    return Bytes.compareTo(data, start, length, other.data, other.start, other.length) == 0;
  }
  @Override
  public byte[] getData() {
    return data;
  }
  @Override
  public int getLength() {
    return length;
  }
  @Override
  public int getStart() {
    return start;
  }
  @Override
  public int hashCode() {
    return hashCode;
  }
}
