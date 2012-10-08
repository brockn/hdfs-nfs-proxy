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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;

import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Mode;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.ModifyTime;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Owner;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.OwnerGroup;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Size;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Type;
import com.google.common.collect.ImmutableMap;

public class FileStatus {
  public final Path path;
  protected  ImmutableMap<Integer, Attribute> mAttrs;
  public FileStatus(Path path, ImmutableMap<Integer, Attribute> attrs) {
    this.path = path;
    mAttrs = attrs;
    assertNotNull(mAttrs);
  }
  public boolean isDir() {
    return ((Type)getAttr(NFS4_FATTR4_TYPE)).getType() == NFS4_DIR;
  }
  public long getSize() {
    return ((Size)getAttr(NFS4_FATTR4_SIZE)).getSize();
  }
  public long getMTime() {
    return toLong(((ModifyTime)getAttr(NFS4_FATTR4_TIME_MODIFY)).getTime());
  }
  public int getMode() {
    return ((Mode)getAttr(NFS4_FATTR4_MODE)).getMode();
  }
  public String getOwner() {
    return ((Owner)getAttr(NFS4_FATTR4_OWNER)).getOwner();
  }
  public String getOwnerGroup() {
    return ((OwnerGroup)getAttr(NFS4_FATTR4_OWNER_GROUP)).getOwnerGroup();
  }
  protected long toLong(Time time) {
    long seconds = time.getSeconds() * 1000L;
    long nanos = time.getNanoSeconds() / 1000000L;
    return seconds + nanos;
  }
  protected Attribute getAttr(int id) {
    Attribute attr = mAttrs.get(id);
    assertNotNull(attr);
    return attr;
  }
}