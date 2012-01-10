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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class WritableFileFileHandleStore extends FileHandleStore {
  protected static final Logger LOGGER = LoggerFactory
      .getLogger(WritableFileFileHandleStore.class);

  protected List<FileHandleStoreEntry> mEntryList = 
      Collections.synchronizedList(new ArrayList<FileHandleStoreEntry>());
  protected DataOutputStream mFileHandleStore;
  protected FileChannel mFileHandleStoreChannel;
  
  @Override
  protected synchronized void initialize() throws IOException {
    
    boolean fileHandleStoreIsBad = false;
    Configuration configuration = getConf();
    File fileHandleStore = new File(configuration.get(
        NFS_FILEHANDLE_STORE_FILE, DEFAULT_NFS_FILEHANDLE_STORE_FILE));
    if (fileHandleStore.isFile()) {
      LOGGER.info("Reading FileHandleStore " + fileHandleStore);
      try {
        FileInputStream fis = new FileInputStream(fileHandleStore);
        DataInputStream is = new DataInputStream(fis);
        try {
          boolean moreEntries = false;
          try {
            moreEntries = is.readBoolean();
          } catch (EOFException e) {
            LOGGER.warn("FileHandleStore was not finished (process probably died/killed)");
            fileHandleStoreIsBad = true;
            moreEntries = false;
          }
          while (moreEntries) {
            FileHandleStoreEntry entry = new FileHandleStoreEntry();
            entry.readFields(is);
            mEntryList.add(entry);            
            LOGGER.info("Read filehandle " + entry.path + " " + entry.fileID);
            try {
              moreEntries = is.readBoolean();
            } catch (EOFException e) {
              LOGGER.warn("FileHandleStore was not finished (process probably died/killed)");
              fileHandleStoreIsBad = true;
              moreEntries = false;
            }
          }
        } finally {
          is.close();
        }
      } catch(IOException ex) {
        throw new IOException("Unable to read filehandle store file: " + fileHandleStore, ex);
      }
    }
    try {
      fileHandleStore.delete();
      FileOutputStream fos = new FileOutputStream(fileHandleStore);
      mFileHandleStoreChannel = fos.getChannel();
      mFileHandleStore = new DataOutputStream(fos);
      Collections.sort(mEntryList);
      for(FileHandleStoreEntry entry : mEntryList) {
        storeFileHandle(entry);
      }
      if(fileHandleStoreIsBad) {
        LOGGER.info("FileHandleStore fixed");
      }
    } catch(IOException ex) {
      throw new IOException("Unable to create filehandle store file: " + fileHandleStore, ex);
    }

  }
  
  @Override
  public synchronized ImmutableList<FileHandleStoreEntry> getAll() {
    return ImmutableList.copyOf(mEntryList);
  }
  
  @Override
  public synchronized void storeFileHandle(FileHandleStoreEntry entry) throws IOException {
    mFileHandleStore.writeBoolean(true);
    entry.write(mFileHandleStore);
    mFileHandleStore.flush();
    mFileHandleStoreChannel.force(true);
    LOGGER.info("Wrote filehandle " + entry.path + "  " + entry.fileID);
  }

  @Override
  public synchronized void close() throws IOException {
    mFileHandleStore.writeBoolean(false);
    mFileHandleStore.flush();
    mFileHandleStoreChannel.force(true);
    mFileHandleStore.close();
    mFileHandleStoreChannel.close();
  }
}
