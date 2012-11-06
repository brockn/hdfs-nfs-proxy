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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.ACCESSRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.ACCESSResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.google.common.annotations.VisibleForTesting;

public class ACCESSHandler extends OperationRequestHandler<ACCESSRequest, ACCESSResponse> {

  protected static final Logger LOGGER = Logger.getLogger(ACCESSHandler.class);
  public static final int ACCESS_READ = 0x04;
  public static final int ACCESS_WRITE = 0x02;
  public static final int ACCESS_EXECUTE = 0x01;

  @VisibleForTesting
  public static int getPerms(int permissions, boolean isOwner) {
    int rtn = 0;
    if (isSet(permissions, ACCESS_READ)) {
      rtn |= NFS_ACCESS_READ;
      rtn |= NFS_ACCESS_LOOKUP;
    }
    if (isSet(permissions, ACCESS_WRITE)) {
      rtn |= NFS_ACCESS_MODIFY;
      rtn |= NFS_ACCESS_EXTEND;
      if (isOwner) {
        rtn |= NFS_ACCESS_DELETE;
      }
    }
    if (isSet(permissions, ACCESS_EXECUTE)) {
      rtn |= NFS_ACCESS_EXECUTE;
    }
    return rtn;
  }

  @VisibleForTesting
  public static int getPermsForUserGroup(String user, String[] groups, FileStatus fileStatus) {
    FsPermission perms = fileStatus.getPermission();
    int permissions = perms.toShort();    
    boolean isOwner = user.equals(fileStatus.getOwner());
    int rtn = getPerms(permissions, isOwner);
    permissions = permissions >> 3;
    for(String group : groups) {
      if (group.equals(fileStatus.getGroup())) {
        rtn |= getPerms(permissions, isOwner);
      }      
    }
    permissions = permissions >> 3;
    if (user.equals(fileStatus.getOwner())) {
      rtn |= getPerms(permissions, isOwner);
    }
    return rtn;
  }
  @VisibleForTesting
  public static boolean isSet(int access, int mode) {
    return (access & mode) == mode;
  }
  @Override
  protected ACCESSResponse createResponse() {
    return new ACCESSResponse();
  }

  @Override
  protected ACCESSResponse doHandle(HDFSState hdfsState, Session session,
      ACCESSRequest request) throws NFS4Exception, IOException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    Path path = hdfsState.getPath(session.getCurrentFileHandle());
    String user = session.getUser();
    String groups[] = session.getGroups();
    FileSystem fs = session.getFileSystem();
    FileStatus fileStatus = fs.getFileStatus(path);
    FsPermission perms = fileStatus.getPermission();
    //FsAction action = perms.getUserAction(); // always comes back ALL??

    int permissions = perms.toShort();
    int saved = permissions;
    int rtn =  getPermsForUserGroup(user, groups, fileStatus);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Checking access for '" + user + "' and path " + path
          + " owned by '" + fileStatus.getOwner()
          + "' permissions " + Integer.toBinaryString(saved)
          + ", Returning " + Integer.toBinaryString(rtn));
    }
    int access = rtn & request.getAccess();
    ACCESSResponse response = createResponse();
    response.setStatus(NFS4_OK);
    response.setAccess(access);
    response.setSupported(access);
    return response;
  }
}
