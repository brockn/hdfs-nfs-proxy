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
package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.util.List;
import java.util.Map;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class GETATTRResponse extends OperationResponse implements Status {
  
  protected int mStatus;
  protected Bitmap mAttrs;
  protected ImmutableList<Attribute> mAttrValues;
  
  @Override
  public void read(RPCBuffer buffer) {
    reset();
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.readAttrs(buffer);
      mAttrs = pair.getFirst();
      mAttrValues = pair.getSecond();
    }
  }
  
  protected void reset() {
    mAttrs = null;
    mAttrValues = null;
  }
  
  // TODO GETATTRResponse to a GETATTRRequest with an empty bitmap
  // results in a packet Linux appears to have no issue with
  // but the packet shows up as malformed in Wireshark. Initial
  // thoughts is the issue was that GETATTRResponse should have
  // the number of bytes transmitted as attr values even if
  // there are zero bytes. However, I remember I had an issue
  // where writing out 0 for attrvalues length caused problems
  // which was solved by a if(!isEmpty()) check in writeAttrs.
  
  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mStatus == NFS4_OK) {
      Attribute.writeAttrs(buffer, mAttrs, mAttrValues);
    }
  }
  public Bitmap getAttrs() {
    return mAttrs;
  }

  public void setAttrs(Bitmap attrs) {
    this.mAttrs = attrs;
  }

  public ImmutableMap<Integer, Attribute> getAttrValues() {
    Map<Integer, Attribute> rtn = Maps.newHashMap();
    for(Attribute attr : mAttrValues) {
      rtn.put(attr.getID(), attr);
    }
    return ImmutableMap.copyOf(rtn);
  }

  public void setAttrValues(List<Attribute> attributes) {
    this.mAttrValues = ImmutableList.copyOf(attributes);
  }

  @Override
  public int getStatus() {
    return mStatus;
  }
  @Override
  public void setStatus(int status) {
    this.mStatus = status;
  }

  @Override
  public int getID() {
    return NFS4_OP_GETATTR;
  }
}
