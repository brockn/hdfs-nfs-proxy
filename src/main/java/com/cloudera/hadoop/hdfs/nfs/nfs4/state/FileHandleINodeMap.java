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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jdbm.PrimaryHashMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.SecondaryHashMap;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class FileHandleINodeMap {

  private final RecordManager mRecordManager;
  private final PrimaryHashMap<FileHandle, INode> mFileHandleINodeMap;
  private final SecondaryHashMap<String, FileHandle, INode> mPathMap;
  private final SecondaryTreeMap<Long, FileHandle, INode> mValidationTimeMap;
  private final ReadWriteLock mReadWriteLock;
  private final Lock mReadLock;
  private final Lock mWriteLock;

  public FileHandleINodeMap(File file) throws IOException {
    Properties properties = new Properties();
    properties.put(RecordManagerOptions.CACHE_TYPE, "mru");
    properties.put(RecordManagerOptions.CACHE_SIZE, "10000");
    properties.put(RecordManagerOptions.COMPRESS, "false");
    mRecordManager = RecordManagerFactory.createRecordManager(file.getAbsolutePath());
    mFileHandleINodeMap = mRecordManager.hashMap("filehandle-to-inode");
    mReadWriteLock = new ReentrantReadWriteLock();
    mReadLock = mReadWriteLock.readLock();
    mWriteLock = mReadWriteLock.writeLock();
    mPathMap = mFileHandleINodeMap.secondaryHashMap("path-index",
        new SecondaryKeyExtractor<String, FileHandle, INode>() {
      @Override
      public String extractSecondaryKey(FileHandle fh, INode inode) {
        return inode.getPath();
      }
    });

    mValidationTimeMap = mFileHandleINodeMap.secondaryTreeMap("validation-time-index",
        new SecondaryKeyExtractor<Long, FileHandle, INode>() {
      @Override
      public Long extractSecondaryKey(FileHandle fh, INode inode) {
        return inode.getCreationTime();
      }
    });
  }
  public void close() throws IOException {
    mRecordManager.close();
  }
  public FileHandle getFileHandleByPath(String path) {
    mReadLock.lock();
    try {
      Iterable<FileHandle> iterable = mPathMap.get(path);
      if(iterable == null) {
        return null;
      }
      Iterator<FileHandle> iterator = iterable.iterator();
      FileHandle fh = iterator.next();
      Preconditions.checkState(!iterator.hasNext());
      return fh;
    } finally {
      mReadLock.unlock();
    }
  }
  public Map<FileHandle, String> getFileHandlesNotValidatedSince(Long upperBound) {
    Map<FileHandle, String> result = Maps.newHashMap();
    mReadLock.lock();
    try {
      SortedMap<Long, Iterable<FileHandle>> map = mValidationTimeMap.headMap(upperBound);
      if(map != null) {
        for(Iterable<FileHandle> fileHandles : map.values()) {
          for(FileHandle fileHandle : fileHandles) {
            result.put(fileHandle, mFileHandleINodeMap.get(fileHandle).getPath());
          }
        }
      }
      return result;
    } finally {
      mReadLock.unlock();
    }
  }
  public INode getINodeByFileHandle(FileHandle fileHandle) {
    mReadLock.lock();
    try {
      return mFileHandleINodeMap.get(fileHandle);
    } finally {
      mReadLock.unlock();
    }
  }
  public void put(FileHandle fileHandle, INode value) throws IOException {
    mWriteLock.lock();
    try {
      mFileHandleINodeMap.put(fileHandle, value);
      mRecordManager.commit();
    } finally {
      mWriteLock.unlock();
    }
  }
  public void remove(FileHandle fileHandle) throws IOException {
    mWriteLock.lock();
    try {
      mFileHandleINodeMap.remove(fileHandle);
      mRecordManager.commit();
    } finally {
      mWriteLock.unlock();
    }
  }
  public void updateValidationTime(Set<FileHandle> fileHandles) throws IOException {
    mWriteLock.lock();
    try {
      for(FileHandle fileHandle : fileHandles) {
        INode inode = mFileHandleINodeMap.get(fileHandle);
        mFileHandleINodeMap.put(fileHandle, new INode(inode.getPath(), inode.getNumber()));
      }
      mRecordManager.commit();
    } finally {
      mWriteLock.unlock();
    }
  }
}
