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

import java.io.IOException;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.google.common.base.Throwables;

public class FileBackedWrite extends AbstractPendingWrite {

  protected static final Logger LOGGER = Logger.getLogger(FileBackedWrite.class);
  private final FileBackedByteArray backingArray;

  public FileBackedWrite(FileBackedByteArray backingArray,
      int xid, long offset, boolean sync) {
    super(xid, offset, sync);
    this.backingArray = backingArray;
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
    FileBackedWrite other = (FileBackedWrite) obj;
    if (getOffset() != other.getOffset()) {
      return false;
    }
    if(hashCode() != other.hashCode()) {
      return false;
    }
    // this is very expensive but should be called only in extremely rare cases
    byte[] data = getData();
    byte[] otherData = other.getData();
    return Bytes.compareTo(data, otherData) == 0;
  }
  @Override
  public byte[] getData() {
    try {
      return backingArray.getByteArray();
    } catch (IOException e) {
      try {
        return backingArray.getByteArray();
      } catch (IOException ex) {
        throw Throwables.propagate(ex);
      }
    }
  }
  public FileBackedByteArray getFileBackedByteArray() {
    return backingArray;
  }
  @Override
  public int getLength() {
    return backingArray.getLength();
  }
  @Override
  public int getStart() {
    return 0;
  }
  @Override
  public int hashCode() {
    return backingArray.getArrayHashCode();
  }
  @Override
  public String toString() {
    return "FileBackedWrite [backingArray=" + backingArray + ", length=" + getLength()
        + ", hashCode=" + hashCode() + "]";
  }
}
