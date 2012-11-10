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
package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;

public class HDFSOutputStream extends OutputStream {

  private final FSDataOutputStream outputStream;
  private final String filename;
  private final FileHandle fileHandle;
  private long position;
  private long lastOperation;
  
  public HDFSOutputStream(FSDataOutputStream outputStream, String filename, FileHandle fileHandle) {
    super();
    this.outputStream = outputStream;
    this.filename = filename;
    this.fileHandle = fileHandle;
    this.position = 0L;
    this.lastOperation = 0L;
  }
  @Override
  public void close() throws IOException {
    outputStream.close();
  }
  public FileHandle getFileHandle() {
    return fileHandle;
  }
  public long getLastOperation() {
    return lastOperation;
  }
  public long getPos()  {
    return position;
  }
  public void sync() throws IOException {
    lastOperation = System.currentTimeMillis();
    outputStream.sync();
  }
  @Override
  public String toString() {
    return "HDFSOutputStream [outputStream=" + outputStream + ", filename="
        + filename + ", position=" + position + "]";
  }
  @Override
  public void write(byte b[]) throws IOException {
    write(b, 0, b.length);
  }
  @Override
  public void write(byte b[], int off, int len) throws IOException {
    outputStream.write(b, off, len);
    position += len;
    lastOperation = System.currentTimeMillis();
  }
  @Override
  public void write(int b) throws IOException {
    outputStream.write(b);
    position++;
    lastOperation = System.currentTimeMillis();
  }
}
