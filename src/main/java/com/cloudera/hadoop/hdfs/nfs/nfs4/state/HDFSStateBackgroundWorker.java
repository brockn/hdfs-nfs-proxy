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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;

public class HDFSStateBackgroundWorker extends Thread {
  protected static final Logger LOGGER = Logger.getLogger(HDFSStateBackgroundWorker.class);
  
  private final ConcurrentMap<FileHandle, HDFSFile> mFileHandleMap;
  private final Map<HDFSOutputStream, WriteOrderHandler> mWriteOrderHandlerMap;
  private final long mIntervalMS;
  private final long mMaxInactivityMS;
  private volatile boolean run;
  
  public HDFSStateBackgroundWorker(Map<HDFSOutputStream, WriteOrderHandler> writeOrderHandlerMap,
      ConcurrentMap<FileHandle, HDFSFile> fileHandleMap, long intervalMS, long maxInactivityMS) {
    mWriteOrderHandlerMap = writeOrderHandlerMap;
    mFileHandleMap = fileHandleMap;
    mIntervalMS = intervalMS;
    mMaxInactivityMS = maxInactivityMS;
    run = true;
    setName("HDFSStateBackgroundWorker-" + getId());
  }
  public void shutdown() {
    run = false;
  }
  public void run() {
    while(run) {
      try {
        TimeUnit.MILLISECONDS.sleep(mIntervalMS);
      } catch (InterruptedException e) {
        // not interruptible
      }
      long minimumLastOperationTime = System.currentTimeMillis() - mMaxInactivityMS;      

      /*
       * First handle write order handlers since they might be using the streams
       */
      Set<HDFSOutputStream> hdfsOutputStreams;
      synchronized (mWriteOrderHandlerMap) {
        hdfsOutputStreams = new HashSet<HDFSOutputStream>(mWriteOrderHandlerMap.keySet());
      }
      for(HDFSOutputStream out : hdfsOutputStreams) {
        if(out.getLastOperation() < minimumLastOperationTime) {
          LOGGER.error("File " + out + " has not been used since " + out.getLastOperation());
          WriteOrderHandler writeOrderHandler;
          synchronized (mWriteOrderHandlerMap) {
            writeOrderHandler = mWriteOrderHandlerMap.remove(out);
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
      /*
       * Next handle the streams themselves.
       */
      Set<FileHandle> fileHandles;
      synchronized (mFileHandleMap) {
        fileHandles = new HashSet<FileHandle>(mFileHandleMap.keySet());
      }
      for(FileHandle fileHandle : fileHandles) {
        HDFSFile file = mFileHandleMap.get(fileHandle);
        if(file != null && file.isOpen()) {
          try {
            file.closeResourcesInactiveSince(minimumLastOperationTime);
          } catch(Exception ex) {
            LOGGER.error("Error thrown trying to close inactive resources in " +file, ex);
          }
        }
      }
    }
  }

}
