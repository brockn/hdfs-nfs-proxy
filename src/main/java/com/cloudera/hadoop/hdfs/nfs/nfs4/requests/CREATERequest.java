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
package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.PathUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.util.List;
import java.util.Map;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


public class CREATERequest extends OperationRequest {

  protected int mType;
  protected String mName;
  protected Bitmap mAttrs;
  protected ImmutableList<Attribute> mAttrValues;
  public Bitmap getAttrs() {
    return mAttrs;
  }

  public ImmutableMap<Integer, Attribute> getAttrValues() {
    Map<Integer, Attribute> rtn = Maps.newHashMap();
    for(Attribute attr : mAttrValues) {
      rtn.put(attr.getID(), attr);
    }
    return ImmutableMap.copyOf(rtn);
  }

  @Override
  public int getID() {
    return NFS4_OP_CREATE;
  }

  public String getName() {
    return mName;
  }
  public int getType() {
    return mType;
  }
  @Override
  public void read(RPCBuffer buffer) {
    mType = buffer.readUint32();
    mName = checkPath(buffer.readString());
    Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.readAttrs(buffer);
    mAttrs = pair.getFirst();
    mAttrValues = pair.getSecond();
  }
  public void setAttrs(Bitmap attrs) {
    this.mAttrs = attrs;
  }
  public void setAttrValues(List<Attribute> attributes) {
    this.mAttrValues = ImmutableList.copyOf(attributes);
  }

  public void setName(String name) {
    mName = name;
  }

  public void setType(int type) {
    mType = type;
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mType);
    buffer.writeString(mName);
    Attribute.writeAttrs(buffer, mAttrs, mAttrValues);
  }
}
