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
package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_OPEN;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ChangeInfo;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;

public class OPENResponse extends OperationResponse implements Status {

  protected int mStatus;
  protected StateID mStateID;
  protected ChangeInfo mChangeInfo;
  protected int mResultFlags;
  protected Bitmap mAttrs;
  protected int mDelgationType;

  @Override
  public void read(RPCBuffer buffer) {
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      mStateID = new StateID();
      mStateID.read(buffer);
      mChangeInfo = new ChangeInfo();
      mChangeInfo.read(buffer);
      mResultFlags = buffer.readUint32();
      mAttrs = Attribute.readAttrsSet(buffer);
      mDelgationType = buffer.readUint32();
    }
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mStatus == NFS4_OK) {
      mStateID.write(buffer);
      mChangeInfo.write(buffer);
      buffer.writeUint32(mResultFlags);
      if(mAttrs == null) {
        mAttrs = new Bitmap();
      }
      Attribute.writeAttrsSet(buffer, mAttrs);
      buffer.writeUint32(mDelgationType);
    }
  }

  public StateID getStateID() {
    return mStateID;
  }

  public void setStateID(StateID stateID) {
    this.mStateID = stateID;
  }

  public ChangeInfo getChangeInfo() {
    return mChangeInfo;
  }

  public void setChangeID(ChangeInfo changeInfo) {
    this.mChangeInfo = changeInfo;
  }

  public int getResultFlags() {
    return mResultFlags;
  }

  public void setResultFlags(int resultFlags) {
    this.mResultFlags = resultFlags;
  }

  public Bitmap getAttrs() {
    return mAttrs;
  }

  public void setAttrs(Bitmap attrs) {
    this.mAttrs = attrs;
  }

  public int getDelgationType() {
    return mDelgationType;
  }

  public void setDelgationType(int delgationType) {
    this.mDelgationType = delgationType;
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
    return NFS4_OP_OPEN;
  }
}
