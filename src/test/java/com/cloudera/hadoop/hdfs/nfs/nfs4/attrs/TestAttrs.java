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
package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.TestUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Time;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class TestAttrs {


  protected static void testAttribute(Attribute base, Attribute copy) {
    copy(base, copy);
    deepEquals(base, copy);
    RPCBuffer buffer = new RPCBuffer();
    copy.write(buffer);
    buffer.flip();
    Attribute parsed = Attribute.parse(buffer, copy.getID());
    assertEquals(base.getClass(), parsed.getClass());
  }

  @Test
  public void testChangeID() throws Exception {
    ChangeID base = new ChangeID();
    base.setChangeID(5L);
    ChangeID copy = new ChangeID();
    testAttribute(base, copy);
  }

  @Test
  public void testFileID() throws Exception {
    FileID base = new FileID();
    base.setFileID(5L);
    FileID copy = new FileID();
    testAttribute(base, copy);
  }

  @Test
  public void testFileSystemID() throws Exception {
    FileSystemID base = new FileSystemID();
    base.setMajor(5L);
    base.setMinor(15L);
    FileSystemID copy = new FileSystemID();
    testAttribute(base, copy);
  }

  @Test
  public void testMode() throws Exception {
    Mode base = new Mode();
    base.setMode(15);
    Mode copy = new Mode();
    testAttribute(base, copy);
  }

  @Test
  public void testModifyTime() throws Exception {
    ModifyTime base = new ModifyTime();
    base.setTime(new Time(5, 15));
    ModifyTime copy = new ModifyTime();
    testAttribute(base, copy);
  }

  @Test
  public void testOwner() throws Exception {
    Owner base = new Owner();
    base.setOwner("brock");
    Owner copy = new Owner();
    testAttribute(base, copy);
  }

  @Test
  public void testOwnerGroup() throws Exception {
    OwnerGroup base = new OwnerGroup();
    base.setOwnerGroup("noland");
    OwnerGroup copy = new OwnerGroup();
    testAttribute(base, copy);
  }


  @Test
  public void testSetAccessTimeClient() throws Exception {
    SetAccessTime base = new SetAccessTime();
    base.setHow(NFS4_SET_TO_CLIENT_TIME4);
    base.setTime(new Time(System.currentTimeMillis()));
    SetAccessTime copy = new SetAccessTime();
    testAttribute(base, copy);
  }

  @Test
  public void testSetAccessTimeServer() throws Exception {
    SetAccessTime base = new SetAccessTime();
    base.setHow(NFS4_SET_TO_SERVER_TIME4);
    SetAccessTime copy = new SetAccessTime();
    testAttribute(base, copy);
  }

  @Test
  public void testSize() throws Exception {
    Size base = new Size();
    base.setSize(15);
    Size copy = new Size();
    testAttribute(base, copy);
  }

  @Test
  public void testType() throws Exception {
    Type base = new Type();
    base.setType(15);
    Type copy = new Type();
    testAttribute(base, copy);
  }
}
