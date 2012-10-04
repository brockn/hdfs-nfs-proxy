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


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class FileSystemIDHandler extends AttributeHandler<FileSystemID> {

  @Override
  public FileSystemID get(HDFSState hdfsState, Session session,
      FileSystem fs, FileStatus fileStatus) {
    FileSystemID fsSystemID = new FileSystemID();
    fsSystemID.setMajor(0L);
    fsSystemID.setMinor(0L);
    return fsSystemID;
  }

}
