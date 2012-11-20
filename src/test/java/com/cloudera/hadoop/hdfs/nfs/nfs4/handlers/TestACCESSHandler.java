/**
 * Copyright 2012 Cloudera Inc.
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
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.UserIDMapper;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.ACCESSRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.ACCESSResponse;
import com.google.common.collect.Lists;

public class TestACCESSHandler extends TestBaseHandler {

  private static class PermTest {
    final String user;
    final String group;
    final FsPermission perm;
    final int result;
    public PermTest(String user, String group, FsPermission perm, int result) {
      this.user = user;
      this.group = group;
      this.perm = perm;
      this.result = result;
    }
    @Override
    public String toString() {
      return "PermTest [user=" + user + ", group=" + group + ", perm=" + perm
          + ", result=" + result + "]";
    }
  }
  public static class TestUserIDMapper extends UserIDMapper implements Configurable {
    Configuration conf;
    @Override
    public Configuration getConf() {
      return conf;
    }
    @Override
    public int getGIDForGroup(String user, int defaultGID)
        throws Exception {
      return conf.getInt(GID, 0);
    }
    @Override
    public String getGroupForGID(int gid,
        String defaultGroup) throws Exception {
      String s = conf.get(GROUP);
      if(s.isEmpty()) {
        return null;
      }
      return s;
    }
    @Override
    public int getUIDForUser(String user, int defaultUID)
        throws Exception {
      return conf.getInt(UID, 0);
    }
    @Override
    public String getUserForUID(int gid, String defaultUser)
        throws Exception {
      String s = conf.get(USER);
      if(s.isEmpty()) {
        return null;
      }
      return s;
    }
    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }
  }
  private static final String USER = "test.user";
  private static final String GROUP = "test.group";

  private static final String UID = "test.uid";
  private static final String GID = "test.gid";

  private ACCESSHandler handler;

  private ACCESSRequest request;
  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new ACCESSHandler();
    request = new ACCESSRequest();
    request.setAccess(ACCESSHandler.ACCESS_READ | ACCESSHandler.ACCESS_WRITE |
        ACCESSHandler.ACCESS_EXECUTE);
    configuration.set(USER_ID_MAPPER_CLASS, TestUserIDMapper.class.getName());

    when(fileStatus.getOwner()).thenReturn("root");
    when(fileStatus.getGroup()).thenReturn("wheel");
    configuration.set(USER, "root");
    configuration.set(GROUP, "wheel");
  }

  @Test
  public void testAllPerms() throws Exception {
    when(filePermissions.toShort()).thenReturn(new FsPermission(FsAction.ALL,
        FsAction.ALL, FsAction.ALL).toShort());
    ACCESSResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    assertEquals(ACCESSHandler.ACCESS_READ | ACCESSHandler.ACCESS_WRITE |
        ACCESSHandler.ACCESS_EXECUTE, response.getAccess());
  }

  @Test
  public void testGetPermsExecute() throws Exception {
    int result1 = ACCESSHandler.getPerms(ACCESSHandler.ACCESS_EXECUTE, true);
    int result2 = ACCESSHandler.getPerms(ACCESSHandler.ACCESS_EXECUTE, false);
    assertEquals(result1, result2);
    assertEquals(NFS_ACCESS_EXECUTE, result1);
  }

  @Test
  public void testGetPermsRead() throws Exception {
    int result1 = ACCESSHandler.getPerms(ACCESSHandler.ACCESS_READ, true);
    int result2 = ACCESSHandler.getPerms(ACCESSHandler.ACCESS_READ, false);
    assertEquals(result1, result2);
    assertEquals(result1, NFS_ACCESS_READ | NFS_ACCESS_LOOKUP);
  }
  @Test
  public void testGetPermsWrite() throws Exception {
    int result1 = ACCESSHandler.getPerms(ACCESSHandler.ACCESS_WRITE, true);
    int result2 = ACCESSHandler.getPerms(ACCESSHandler.ACCESS_WRITE, false);
    assertFalse(result1 == result2);
    assertEquals(NFS_ACCESS_MODIFY | NFS_ACCESS_EXTEND | NFS_ACCESS_DELETE, result1);
    assertEquals(NFS_ACCESS_MODIFY | NFS_ACCESS_EXTEND, result2);
  }


  @Test
  public void testNoPerms() throws Exception {
    ACCESSResponse response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
    assertEquals(0, response.getAccess());
  }


  @Test
  public void testPerms() throws Exception {
    List<PermTest> perms = Lists.newArrayList();
    // read for owner when owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.READ, FsAction.NONE,  FsAction.NONE),
        NFS_ACCESS_READ | NFS_ACCESS_LOOKUP));
    // read for group when owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.NONE, FsAction.READ,  FsAction.NONE),
        NFS_ACCESS_READ | NFS_ACCESS_LOOKUP));
    // read for other when owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.READ),
        NFS_ACCESS_READ | NFS_ACCESS_LOOKUP));
    // read for other when not owner
    perms.add(new PermTest("notroot", "wheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.READ),
        NFS_ACCESS_READ | NFS_ACCESS_LOOKUP));
    // read for other when not owner
    perms.add(new PermTest("root", "notwheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.READ),
        NFS_ACCESS_READ | NFS_ACCESS_LOOKUP));
    // read for other when not owner or group
    perms.add(new PermTest("notroot", "notwheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.READ),
        NFS_ACCESS_READ | NFS_ACCESS_LOOKUP));

    // write for owner when owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.WRITE, FsAction.NONE,  FsAction.NONE),
        NFS_ACCESS_MODIFY | NFS_ACCESS_EXTEND | NFS_ACCESS_DELETE));
    // write for group when owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.NONE, FsAction.WRITE,  FsAction.NONE),
        NFS_ACCESS_MODIFY | NFS_ACCESS_EXTEND | NFS_ACCESS_DELETE));
    // write for other when owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.WRITE),
        NFS_ACCESS_MODIFY | NFS_ACCESS_EXTEND | NFS_ACCESS_DELETE));
    // write for other when not owner
    perms.add(new PermTest("notroot", "wheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.WRITE),
        NFS_ACCESS_MODIFY | NFS_ACCESS_EXTEND));
    // write for other when not owner
    perms.add(new PermTest("root", "notwheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.WRITE),
        NFS_ACCESS_MODIFY | NFS_ACCESS_EXTEND | NFS_ACCESS_DELETE));
    // write for other when not owner or group
    perms.add(new PermTest("notroot", "notwheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.WRITE),
        NFS_ACCESS_MODIFY | NFS_ACCESS_EXTEND));

    // execute for owner when owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.EXECUTE, FsAction.NONE,  FsAction.NONE),
        NFS_ACCESS_EXECUTE));
    // execute for group when owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.NONE, FsAction.EXECUTE,  FsAction.NONE),
        NFS_ACCESS_EXECUTE));
    // execute for other when owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.EXECUTE),
        NFS_ACCESS_EXECUTE));
    // execute for other when not owner
    perms.add(new PermTest("notroot", "wheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.EXECUTE),
        NFS_ACCESS_EXECUTE));
    // execute for other when not owner
    perms.add(new PermTest("root", "notwheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.EXECUTE),
        NFS_ACCESS_EXECUTE));
    // execute for other when not owner or group
    perms.add(new PermTest("notroot", "notwheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.EXECUTE),
        NFS_ACCESS_EXECUTE));
    // no perms but owner, this might be rethought?
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.NONE),
        0));
    // all for user/group but not user/groups
    perms.add(new PermTest("notroot", "notwheel",
        new FsPermission(FsAction.ALL, FsAction.ALL,  FsAction.NONE),
        0));
    // all for user/group but not user/group
    perms.add(new PermTest("notroot", "wheel",
        new FsPermission(FsAction.ALL, FsAction.NONE,  FsAction.NONE),
        0));
    // owner has all, is owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.ALL, FsAction.NONE,  FsAction.NONE),
        NFS_ACCESS_READ | NFS_ACCESS_LOOKUP | NFS_ACCESS_MODIFY |
        NFS_ACCESS_EXTEND | NFS_ACCESS_DELETE | NFS_ACCESS_EXECUTE));
    // group has all is owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.NONE, FsAction.ALL,  FsAction.NONE),
        NFS_ACCESS_READ | NFS_ACCESS_LOOKUP | NFS_ACCESS_MODIFY |
        NFS_ACCESS_EXTEND | NFS_ACCESS_DELETE | NFS_ACCESS_EXECUTE));
    // other has all is owner
    perms.add(new PermTest("root", "wheel",
        new FsPermission(FsAction.NONE, FsAction.NONE,  FsAction.ALL),
        NFS_ACCESS_READ | NFS_ACCESS_LOOKUP | NFS_ACCESS_MODIFY |
        NFS_ACCESS_EXTEND | NFS_ACCESS_DELETE | NFS_ACCESS_EXECUTE));

    for(PermTest permTest : perms) {
      when(filePermissions.toShort()).thenReturn(permTest.perm.toShort());
      int result = ACCESSHandler.getPermsForUserGroup(permTest.user,
          new String[] {permTest.group}, fileStatus);
      assertEquals(permTest.toString(),
          Integer.toBinaryString(permTest.result),
          Integer.toBinaryString(result));
    }
  }
}
