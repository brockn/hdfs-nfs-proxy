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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.hadoop.hdfs.nfs.PathUtils;
import com.google.common.collect.Maps;

public class PendingWriteFactory {

  private final File[] mTempDirs;
  private final long mMaxFileSize;
  private final BlockingQueue<FileHandle> mTempFileQueue = new LinkedBlockingQueue<FileHandle>();
  private final Map<File, AtomicLong> mTempFileReferenceCounts = Maps.newHashMap();

  public PendingWriteFactory(File[] tempDirs, long maxFileSize) {
    mTempDirs = Arrays.copyOf(tempDirs, tempDirs.length);
    mMaxFileSize = maxFileSize;
  }

  public PendingWrite create(long currentPos, int xid, long offset,
      boolean sync, byte[] data, int start, int length) throws IOException {
    // if the write is more than 1MB into the
    // future, store the write in a file
    if(offset > (currentPos + ONE_MB)) {
      FileBackedByteArray backingArray = writeToTemp(offset, data, start, length);
      return new FileBackedWrite(backingArray, xid, offset, sync);
    }
    return new MemoryBackedWrite(xid, offset, sync, data, start, length);
  }

  public void destroy(PendingWrite write) {
    if(write instanceof FileBackedWrite) {
      File file = ((FileBackedWrite)write).getFileBackedByteArray().getFile();
      boolean delete = false;
      synchronized (mTempFileReferenceCounts) {
        long count = mTempFileReferenceCounts.get(file).decrementAndGet();
        if(count <= 0) {
          mTempFileReferenceCounts.remove(file);
          delete = true;
        }
      }
      if(delete) {
        PathUtils.fullyDelete(file);
      }
    }
  }

  private FileBackedByteArray writeToTemp(long offset, byte[] buffer, int start, int length)
      throws IOException {
    FileHandle fileHandle = mTempFileQueue.poll();
    if(fileHandle == null) {
      fileHandle = nextTempFile(offset);
    }
    if(fileHandle.file.length() > mMaxFileSize) {
      fileHandle.randomAccessFile.close();
      fileHandle = nextTempFile(offset);
    }
    FileBackedByteArray result = FileBackedByteArray.create(fileHandle.file,
        fileHandle.randomAccessFile, buffer, start, length);
    mTempFileQueue.add(fileHandle);
    synchronized (mTempFileReferenceCounts) {
      mTempFileReferenceCounts.get(fileHandle.file).incrementAndGet();
    }
    return result;
  }
  private FileHandle nextTempFile(long offset) throws IOException {
    int fileIndex = ((int)(offset) & Integer.MAX_VALUE) % mTempDirs.length;
    File base = mTempDirs[fileIndex];
    File file = new File(base, UUID.randomUUID().toString());
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
    FileHandle fileHandle = new FileHandle(file, randomAccessFile);
    synchronized (mTempFileReferenceCounts) {
      mTempFileReferenceCounts.put(fileHandle.file, new AtomicLong(0));
    }
    return fileHandle;
  }
  private static class FileHandle {
    private final File file;
    private final RandomAccessFile randomAccessFile;
    private FileHandle(File file, RandomAccessFile randomAccessFile) {
      this.file = file;
      this.randomAccessFile = randomAccessFile;
    }
  }
}