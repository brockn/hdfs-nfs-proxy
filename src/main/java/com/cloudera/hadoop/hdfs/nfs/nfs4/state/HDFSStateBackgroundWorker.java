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
package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class HDFSStateBackgroundWorker extends Thread {
  protected static final Logger LOGGER = Logger.getLogger(HDFSStateBackgroundWorker.class);
  
  private final HDFSState mHDFSState;
  private final ConcurrentMap<FileHandle, HDFSFile> mOpenFileMap;
  private final Map<FileHandle, WriteOrderHandler> mWriteOrderHandlerMap;
  private final FileHandleINodeMap mFileHandleINodeMap;
  private final long mIntervalMS;
  private final long mMaxInactivityMS;
  private final long mFileHandleReapInterval;
  private volatile boolean run;
  
  public HDFSStateBackgroundWorker(HDFSState hdfsState,
      Map<FileHandle, WriteOrderHandler> writeOrderHandlerMap,
      ConcurrentMap<FileHandle, HDFSFile> openFileMap, FileHandleINodeMap fileHandleINodeMap,
      long intervalMS, long maxInactivityMS, long fileHandleReapInterval) {
    mHDFSState = hdfsState;
    mWriteOrderHandlerMap = writeOrderHandlerMap;
    mOpenFileMap = openFileMap;
    mFileHandleINodeMap = fileHandleINodeMap;
    mIntervalMS = intervalMS;
    mMaxInactivityMS = maxInactivityMS;
    mFileHandleReapInterval = fileHandleReapInterval;
    run = true;
    setName("HDFSStateBackgroundWorker-" + getId());
  }
  public void shutdown() {
    run = false;
  }
  @Override
  public void run() {
    while(run) {
      try {
        TimeUnit.MILLISECONDS.sleep(mIntervalMS);
      } catch (InterruptedException e) {
        // not interruptible
      }
      long minimumLastOperationTime = System.currentTimeMillis() - mMaxInactivityMS;      
      /*
       * We must close inactive streams first so that because in WRITEHandler we use
       *  the stream to get the WriteOrderHandler and if we remove the handler first
       *  it's possible a handler for the stream which we are about the close would be
       *  started. Clearly we need to cleanup this design.
       */
      Set<FileHandle> fileHandles;
      synchronized (mOpenFileMap) {
        fileHandles = new HashSet<FileHandle>(mOpenFileMap.keySet());
      }
      for(FileHandle fileHandle : fileHandles) {
        HDFSFile file = mOpenFileMap.get(fileHandle);
        if(file != null && file.isOpen()) {
          try {
            file.closeResourcesInactiveSince(minimumLastOperationTime);
          } catch(Exception ex) {
            LOGGER.error("Error thrown trying to close inactive resources in " +file, ex);
          }
        }
      }
      /*
       * Now remove write order handlers
       */
      synchronized (mWriteOrderHandlerMap) {
        fileHandles = new HashSet<FileHandle>(mWriteOrderHandlerMap.keySet());
      }
      for(FileHandle fileHandle : fileHandles) {
        HDFSFile file = mOpenFileMap.get(fileHandle);
        Preconditions.checkState(file != null);
        OpenResource<HDFSOutputStream> resource = file.getHDFSOutputStream();
        if(resource != null) {
          HDFSOutputStream out = resource.get();
          if(out.getLastOperation() < minimumLastOperationTime) {
            LOGGER.error("File " + out + " has not been used since " + out.getLastOperation());
            WriteOrderHandler writeOrderHandler;
            synchronized (mWriteOrderHandlerMap) {
              writeOrderHandler = mWriteOrderHandlerMap.remove(fileHandle);
            }
            if(writeOrderHandler != null) {
              try {
                writeOrderHandler.close(true);
              } catch (Exception e) {
                LOGGER.error("Error thrown trying to close " + out, e);
              }
            }
          }
        }
      }
      /*
       * Now we are going to reap any file handles for old non-existent files
       */
      Long upperBound = System.currentTimeMillis() - mFileHandleReapInterval;
      Map<FileHandle, String> agedFileHandles = 
          mFileHandleINodeMap.getFileHandlesNotValidatedSince(upperBound);
      Set<FileHandle> fileHandlesWhichStillExist = Sets.newHashSet();
      for(FileHandle fileHandle : agedFileHandles.keySet()) {
        if(!run) {
          break;
        }
        String path = agedFileHandles.get(fileHandle);
        try {
          if(!mHDFSState.deleteFileHandleFileIfNotExist(fileHandle)) {
            fileHandlesWhichStillExist.add(fileHandle);
          }
          // expensive logic so do this slowly
          TimeUnit.MILLISECONDS.sleep(10L);
        } catch (IOException e) {
          LOGGER.warn("Error checking existance of " + path, e);
        } catch (InterruptedException e) {
          // not interruptible
        }
      }
      mFileHandleINodeMap.updateValidationTime(fileHandlesWhichStillExist);
    }
  }
}