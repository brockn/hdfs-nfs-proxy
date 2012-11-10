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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class SupportedAttributes extends Attribute {
  protected Bitmap mAttrs;
  public SupportedAttributes() {
    super();
  }
  public Bitmap getAttrs() {
    return mAttrs;
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_SUPPORTED_ATTRS;
  }

  @Override
  public void read(RPCBuffer buffer) {
    mAttrs = new Bitmap();
    mAttrs.read(buffer);
  }

  public void setAttrs(Bitmap attrs) {
    this.mAttrs = attrs;
  }

  @Override
  public void write(RPCBuffer buffer) {
    mAttrs.write(buffer);
  }



}
