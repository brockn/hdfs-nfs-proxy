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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.DEFAULT_NFS_FILEHANDLE_STORE_FILE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS_FILEHANDLE_STORE_FILE;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class WritableFileFileHandleStore extends FileHandleStore {
  protected static final Logger LOGGER = Logger
      .getLogger(WritableFileFileHandleStore.class);

  File mFileHandleStoreFile;

  protected DataOutputStream mFileHandleStore;
  protected FileChannel mFileHandleStoreChannel;

  @Override
  protected synchronized void initialize() throws IOException {

    Configuration configuration = getConf();

    mFileHandleStoreFile= new File(configuration.get(
        NFS_FILEHANDLE_STORE_FILE, DEFAULT_NFS_FILEHANDLE_STORE_FILE));

    Pair<List<FileHandleStoreEntry>, Boolean> pair = readFile();
    boolean fileHandleStoreIsBad = pair.getSecond();

    try {
      mFileHandleStoreFile.delete();
      FileOutputStream fos = new FileOutputStream(mFileHandleStoreFile);
      mFileHandleStoreChannel = fos.getChannel();
      mFileHandleStore = new DataOutputStream(fos);
      List<FileHandleStoreEntry> entryList = pair.getFirst();
      Collections.sort(entryList);
      for(FileHandleStoreEntry entry : entryList) {
        storeFileHandle(entry);
      }
      if(fileHandleStoreIsBad) {
        LOGGER.info("FileHandleStore fixed");
      }
    } catch(IOException ex) {
      throw new IOException("Unable to create filehandle store file: " + mFileHandleStoreFile, ex);
    }

  }

  protected Pair<List<FileHandleStoreEntry>, Boolean> readFile() throws IOException {
    List<FileHandleStoreEntry> entryList = Lists.newArrayList();
    boolean fileHandleStoreIsBad = false;
    if (mFileHandleStoreFile.isFile()) {
      LOGGER.info("Reading FileHandleStore " + mFileHandleStoreFile);
      try {
        FileInputStream fis = new FileInputStream(mFileHandleStoreFile);
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
            entryList.add(entry);
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
        throw new IOException("Unable to read filehandle store file: " + mFileHandleStoreFile, ex);
      }
    }
    return Pair.of(entryList, fileHandleStoreIsBad);
  }

  @Override
  public synchronized ImmutableList<FileHandleStoreEntry> getAll() {
    try {
      return ImmutableList.copyOf(readFile().getFirst());
    } catch (IOException e) {
      LOGGER.warn("Exception reading filehandle store file: " + mFileHandleStoreFile, e);
    }
    return ImmutableList.<FileHandleStoreEntry>of();
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
