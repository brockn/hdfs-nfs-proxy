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
package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;



import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;

public class SizeHandler extends AttributeHandler<Size> {

  @Override
  public Size get(NFS4Handler server, Session session, FileSystem fs,
      FileStatus fileStatus) throws NFS4Exception {
    Size size = new Size();
    size.setSize(server.getFileSize(fileStatus));
    return size;
  }
  /* Linux uses SETATTR (size = 0) to truncate files.
   */
  public boolean set(NFS4Handler server, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, Size size)
      throws NFS4Exception, IOException {
    // we only support truncating files
    if(size.getSize() != 0) {
      throw new UnsupportedOperationException("Setting size to non-zero (truncate) is not supported.");
    }
    // open the file, overwriting if needed. Creation of an empty file with
    // overwrite on is the only way we can support truncating files
    FSDataOutputStream out = server.forWrite(stateID, fs, session.getCurrentFileHandle(), true);
    if(out.getPos() != 0) {
      stateID = server.close(session.getSessionID(), stateID, stateID.getSeqID(), session.getCurrentFileHandle());
      out = server.forWrite(stateID, fs, session.getCurrentFileHandle(), true); 
    }
    out.sync();
    return true;
  }

}
