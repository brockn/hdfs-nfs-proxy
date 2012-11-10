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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.TestUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.util.BitSet;

import junit.framework.Assert;

import org.junit.Test;

public class TestBitmap {

  private void equals(BitSet bs1, BitSet bs2) {
    Assert.assertEquals(bs1.size(), bs2.size());
    Assert.assertEquals(bs1.cardinality(), bs2.cardinality());
    for (int bitIndex = 0; bitIndex < bs1.size(); bitIndex++) {
      Assert.assertEquals(bs1.get(bitIndex), bs2.get(bitIndex));
    }
  }

  @Test
  public void testEmpty() {
    Bitmap base = new Bitmap();
    Bitmap copy = new Bitmap();
    copy(base, copy);
    equals(base.getMask(), copy.getMask());
  }
  
  @Test
  public void testLarge() {
    // both first int and second int in bitmap
    Bitmap base = new Bitmap();
    base.set(NFS4_FATTR4_SUPPORTED_ATTRS);
    base.set(NFS4_FATTR4_TYPE);
    base.set(NFS4_FATTR4_CHANGE);
    base.set(NFS4_FATTR4_SIZE);
    base.set(NFS4_FATTR4_FSID);
    base.set(NFS4_FATTR4_FILEID);
    base.set(NFS4_FATTR4_MODE);
    base.set(NFS4_FATTR4_NUMLINKS);
    base.set(NFS4_FATTR4_OWNER);
    base.set(NFS4_FATTR4_OWNER_GROUP);
    base.set(NFS4_FATTR4_RAWDEV);
    base.set(NFS4_FATTR4_SPACE_USED);
    base.set(NFS4_FATTR4_TIME_ACCESS);
    base.set(NFS4_FATTR4_TIME_METADATA);
    base.set(NFS4_FATTR4_TIME_MODIFY);
    base.set(64);
    Bitmap copy = new Bitmap();
    copy(base, copy);
    equals(base.getMask(), copy.getMask());
  }
  
  @Test
  public void testNotDivisableBy32() {
    // first int only
    Bitmap base = new Bitmap();
    base.set(1);
    base.set(4);
    base.set(5);
    base.set(99);
    Bitmap copy = new Bitmap();
    copy(base, copy);
    equals(base.getMask(), copy.getMask());
  }
  @Test
  public void testSmall() {
    // first int only
    Bitmap base = new Bitmap();
    base.set(NFS4_FATTR4_LEASE_TIME);
    base.set(NFS4_FATTR4_MAXFILESIZE);
    base.set(NFS4_FATTR4_MAXREAD);
    base.set(NFS4_FATTR4_MAXWRITE); // 31 (had bug where 31 was not handled)
    base.set(64);
    Bitmap copy = new Bitmap();
    copy(base, copy);
    equals(base.getMask(), copy.getMask());
  }
  @Test
  public void testToString() {
    Bitmap base = new Bitmap();
    base.set(NFS4_FATTR4_SUPPORTED_ATTRS);
    base.set(NFS4_FATTR4_SPACE_USED);
    Assert.assertEquals("{0, 45}", base.toString());
  }
}
