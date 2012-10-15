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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class FileHandleStoreEntry implements WritableComparable<FileHandleStoreEntry> {
  private byte[] fileHandle;
  private String path;
  private long fileID;

  public FileHandleStoreEntry() {
    this(null, null, -1);
  }

  public byte[] getFileHandle() {
    return fileHandle;
  }

  public String getPath() {
    return path;
  }

  public long getFileID() {
    return fileID;
  }

  public FileHandleStoreEntry(byte[] fileHandle, String path, long fileID) {
    this.fileHandle = fileHandle;
    this.path = path;
    this.fileID = fileID;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(fileHandle.length);
    out.write(fileHandle);
    out.writeUTF(path);
    out.writeLong(fileID);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    fileHandle = new byte[length];
    in.readFully(fileHandle);
    path = in.readUTF();
    fileID = in.readLong();
  }

  @Override
  public int compareTo(FileHandleStoreEntry o) {
    return path.compareTo(o.path);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(fileHandle);
    result = prime * result + (int) (fileID ^ (fileID >>> 32));
    result = prime * result + ((path == null) ? 0 : path.hashCode());
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
    FileHandleStoreEntry other = (FileHandleStoreEntry) obj;
    if (!Arrays.equals(fileHandle, other.fileHandle))
      return false;
    if (fileID != other.fileID)
      return false;
    if (path == null) {
      if (other.path != null)
        return false;
    } else if (!path.equals(other.path))
      return false;
    return true;
  }

}