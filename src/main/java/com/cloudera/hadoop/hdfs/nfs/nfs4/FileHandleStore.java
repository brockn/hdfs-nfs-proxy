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
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.ImmutableList;

public abstract class FileHandleStore extends Configured {
  
  public static FileHandleStore get(Configuration conf) {
    FileHandleStore fileHandleStore;
    fileHandleStore = ReflectionUtils.newInstance(
        conf.getClass(NFS_FILEHANDLE_STORE_CLASS, WritableFileFileHandleStore.class)
        .asSubclass(FileHandleStore.class), conf);
    try {
      fileHandleStore.initialize();
    } catch(Exception ex) {
      throw new RuntimeException(ex);
    }
    return fileHandleStore;
  }
  
  protected abstract void initialize() throws Exception;
  /**
   * @return an ImmutableList of all entries in the store
   */
  public abstract ImmutableList<FileHandleStoreEntry> getAll();
  /**
   * Persist an entry to store. If this method returns without throwing
   * an exception, the entry must have been written to a persistent 
   * location.
   * 
   * @param entry
   * @throws IOException
   */
  public abstract void storeFileHandle(FileHandleStoreEntry entry) throws IOException;
  /**
   * Close the store. Once called no additional methods on this 
   * object should be called. This method may legally called more
   * than once.
   * 
   * @throws IOException
   */
  public abstract void close() throws IOException;
}
