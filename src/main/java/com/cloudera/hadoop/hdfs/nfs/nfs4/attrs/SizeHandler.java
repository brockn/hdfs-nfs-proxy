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
package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;



import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSOutputStream;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.cloudera.hadoop.hdfs.nfs.rpc.LRUCache;

public class SizeHandler extends AttributeHandler<Size> {

  /**
   * Store the last 1000 truncate requests so we don't process
   * one twice. Not perfect but should work well enough.
   */
  protected LRUCache<Integer, Object> mProcessedRequests = new LRUCache<Integer, Object>(1000);
  protected final Object value = new Object();

  @Override
  public Size get(HDFSState hdfsState, Session session, FileSystem fs,
      FileStatus fileStatus) throws NFS4Exception {
    Size size = new Size();
    size.setSize(hdfsState.getFileSize(fileStatus));
    return size;
  }
  /* Linux uses SETATTR (size = 0) to truncate files.
   */
  @Override
  public boolean set(HDFSState hdfsState, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, Size size)
      throws NFS4Exception, IOException {
    // we only support truncating files
    if(size.getSize() != 0) {
      throw new UnsupportedOperationException("Setting size to non-zero (truncate) is not supported.");
    }
    synchronized (mProcessedRequests) {
      if(mProcessedRequests.containsKey(session.getXID())) {
        return true;
      }
      mProcessedRequests.put(session.getXID(), value);
      // open the file, overwriting if needed. Creation of an empty file with
      // overwrite on is the only way we can support truncating files
      HDFSOutputStream out = hdfsState.openForWrite(stateID, session.getCurrentFileHandle(), true);
      out.sync();
      return true;

    }
  }

}
