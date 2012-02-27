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


import java.util.Arrays;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

/**
 * Represents a NFS FileHandle.
 */
public class FileHandle implements MessageBase {
  protected byte[] mBytes;
  public FileHandle() {

  }
  public FileHandle(byte[] bytes) {
    this.mBytes = bytes;
  }
  @Override
  public void read(RPCBuffer buffer) {
    mBytes = buffer.readBytes();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mBytes.length);
    buffer.writeBytes(mBytes);
  }

  public byte[] getBytes() {
    return mBytes;
  }

  public void setBytes(byte[] bytes) {
    this.mBytes = bytes;
  }

  @Override
  public String toString() {
    return "FileHandle [mBytes=" + Arrays.toString(mBytes) + "]";
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(mBytes);
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
    FileHandle other = (FileHandle) obj;
    if (!Arrays.equals(mBytes, other.mBytes))
      return false;
    return true;
  }
}
