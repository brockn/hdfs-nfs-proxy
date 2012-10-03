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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.TestUtils.copy;
import static com.cloudera.hadoop.hdfs.nfs.TestUtils.deepEquals;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_CHANGE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_FILEID;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_FSID;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_LEASE_TIME;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_MAXFILESIZE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_MAXREAD;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_MAXWRITE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_MODE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_NUMLINKS;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_OWNER;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_OWNER_GROUP;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_RAWDEV;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_SIZE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_SPACE_USED;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_SUPPORTED_ATTRS;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_TIME_ACCESS;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_TIME_METADATA;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_TIME_MODIFY;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_FATTR4_TYPE;

import org.junit.Test;

public class TestBitmap {

  @Test
  public void testBitMapLarge() {
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
    deepEquals(base, copy);
  }

  @Test
  public void testBitMapSmall() {
    // first int only
    Bitmap base = new Bitmap();
    base.set(NFS4_FATTR4_LEASE_TIME);
    base.set(NFS4_FATTR4_MAXFILESIZE);
    base.set(NFS4_FATTR4_MAXREAD);
    base.set(NFS4_FATTR4_MAXWRITE); // 31 (had bug where 31 was not handled)
    base.set(64);
    Bitmap copy = new Bitmap();
    copy(base, copy);
    deepEquals(base, copy);
  }
}
