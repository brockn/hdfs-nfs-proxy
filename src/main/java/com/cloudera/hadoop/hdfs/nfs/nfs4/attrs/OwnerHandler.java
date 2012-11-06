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

import com.cloudera.hadoop.hdfs.nfs.NetUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;

public class OwnerHandler extends AttributeHandler<Owner> {

  public static String removeDomain(String user) {
    int pos;
    if((pos = user.indexOf('@')) > 0 && pos < user.length()) {
      return user.substring(pos + 1);
    }
    return user;
  }

  @Override
  public Owner get(HDFSState hdfsState, Session session, FileSystem fs,
      FileStatus fileStatus) {
    Owner owner = new Owner();
    String domain = NetUtils.getDomain(session.getConfiguration(), session.getClientAddress());
    owner.setOwner(fileStatus.getOwner() + "@" + domain);
    return owner;
  }

  @Override
  public boolean set(HDFSState hdfsState, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, Owner attr)
      throws IOException {
    String user = removeDomain(attr.getOwner());
    if(fileStatus.getOwner().equals(user)) {
      fs.setOwner(fileStatus.getPath(), user, null);
      return true;
    }
    return false;
  }
}
