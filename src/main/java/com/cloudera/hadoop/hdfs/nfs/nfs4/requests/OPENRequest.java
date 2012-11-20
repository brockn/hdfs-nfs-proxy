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
package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.PathUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class OPENRequest extends OperationRequest {

  protected int mSeqID;
  protected int mAccess;
  protected int mDeny;
  protected long mClientID;
  protected OpaqueData mOwner;
  protected int mOpenType;
  protected int mCreateMode;

  protected Bitmap mAttrs;
  protected ImmutableList<Attribute> mAttrValues;
  protected OpaqueData8 mVerifer;

  protected int mClaimType;
  protected String mName;

  public int getAccess() {
    return mAccess;
  }

  public Bitmap getAttrs() {
    return mAttrs;
  }

  public ImmutableList<Attribute> getAttrValues() {
    return mAttrValues;
  }

  public int getClaimType() {
    return mClaimType;
  }

  public long getClientID() {
    return mClientID;
  }

  public int getCreateMode() {
    return mCreateMode;
  }

  public int getDeny() {
    return mDeny;
  }

  @Override
  public int getID() {
    return NFS4_OP_OPEN;
  }

  public String getName() {
    return mName;
  }

  public int getOpenType() {
    return mOpenType;
  }

  public OpaqueData getOwner() {
    return mOwner;
  }

  public int getSeqID() {
    return mSeqID;
  }

  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  @Override
  public void read(RPCBuffer buffer) {
    mSeqID = buffer.readUint32();
    mAccess = buffer.readUint32();
    mDeny = buffer.readUint32();
    mClientID = buffer.readUint64();
    mOwner = new OpaqueData(buffer.readUint32());
    mOwner.read(buffer);
    mOpenType = buffer.readUint32();
    if(mOpenType == NFS4_OPEN4_CREATE) {
      mCreateMode = buffer.readUint32();
      if(mCreateMode == NFS4_CREATE_EXCLUSIVE4) {
        mVerifer = new OpaqueData8();
        mVerifer.read(buffer);
      } else {
        Preconditions.checkArgument((mCreateMode == NFS4_CREATE_UNCHECKED4) ||
            (mCreateMode == NFS4_CREATE_GUARDED4), Integer.toHexString(mCreateMode));
        Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.readAttrs(buffer);
        mAttrs = pair.getFirst();
        mAttrValues = pair.getSecond();
      }
    }
    mClaimType = buffer.readUint32();
    if(mClaimType != NFS4_CLAIM_NULL) {
      throw new UnsupportedOperationException("CLAIM_NULL is only claim supported: " + mClaimType);
    }
    mName = checkPath(buffer.readString());
  }

  public void setAccess(int access) {
    this.mAccess = access;
  }

  public void setAttrs(Bitmap attrs) {
    this.mAttrs = attrs;
  }

  public void setAttrValues(ImmutableList<Attribute> attrValues) {
    this.mAttrValues = attrValues;
  }

  public void setClaimType(int claimType) {
    this.mClaimType = claimType;
  }

  public void setClientID(long clientID) {
    this.mClientID = clientID;
  }

  public void setCreateMode(int createMode) {
    this.mCreateMode = createMode;
  }

  public void setDeny(int deny) {
    this.mDeny = deny;
  }

  public void setName(String name) {
    this.mName = name;
  }

  public void setOpenType(int openType) {
    this.mOpenType = openType;
  }

  public void setOwner(OpaqueData owner) {
    this.mOwner = owner;
  }

  public void setSeqID(int seqID) {
    this.mSeqID = seqID;
  }

  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mSeqID);
    buffer.writeUint32(mAccess);
    buffer.writeUint32(mDeny);
    buffer.writeUint64(mClientID);
    buffer.writeUint32(mOwner.getSize());
    mOwner.write(buffer);
    buffer.writeUint32(mOpenType);
    if(mOpenType == NFS4_OPEN4_CREATE) {
      buffer.writeUint32(mCreateMode);
      if(mCreateMode == NFS4_CREATE_EXCLUSIVE4) {
        mVerifer.write(buffer);
      } else {
        Preconditions.checkArgument((mCreateMode == NFS4_CREATE_UNCHECKED4) ||
            (mCreateMode == NFS4_CREATE_GUARDED4), Integer.toHexString(mCreateMode));
        Attribute.writeAttrs(buffer, mAttrs, mAttrValues);
      }
    }
    if(mClaimType != NFS4_CLAIM_NULL) {
      throw new UnsupportedOperationException("CLAIM_NULL is only claim supported");
    }
    buffer.writeUint32(mClaimType);
    buffer.writeString(mName);
  }
}
