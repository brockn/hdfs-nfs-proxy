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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.NFSUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class OwnerGroupHandler extends AttributeHandler<OwnerGroup> {

  @Override
  public OwnerGroup get(HDFSState hdfsState, Session session, FileSystem fs,
      FileStatus fileStatus) {
    OwnerGroup ownerGroup = new OwnerGroup();
    String domain = NFSUtils.getDomain(session.getConfiguration(), session.getClientAddress());
    ownerGroup.setOwnerGroup(fileStatus.getGroup() + "@" + domain);
    return ownerGroup;
  }

  @Override
  public boolean set(HDFSState hdfsState, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, OwnerGroup attr) throws IOException {
    String group = OwnerHandler.removeDomain(attr.getOwnerGroup());
    if(fileStatus.getGroup().equals(group)) {
      fs.setOwner(fileStatus.getPath(), null, group);
      return true;
    }
    return false;
  }
}
