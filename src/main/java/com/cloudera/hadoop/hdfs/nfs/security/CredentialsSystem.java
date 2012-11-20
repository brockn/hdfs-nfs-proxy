/**
 * Copyright 2012 Cloudera Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

/**
 * Implementation of RPC AUTH_SYS
 */
public class CredentialsSystem extends AuthenticatedCredentials {

  protected static final Logger LOGGER = Logger.getLogger(CredentialsSystem.class);


  protected int mUID, mGID;
  protected int[] mAuxGIDs;
  protected String mHostName;
  protected int mStamp;

  public CredentialsSystem() {
    super();
    this.mCredentialsLength = 0;
    this.mHostName = HOSTNAME;
  }


  public int[] getAuxGIDs() {
    return mAuxGIDs;
  }

  @Override
  public int getFlavor() {
    return RPC_AUTH_UNIX;
  }

  public int getGID() {
    return mGID;
  }

  public int getUID() {
    return mUID;
  }

  @Override
  public void read(RPCBuffer buffer) {
    mCredentialsLength = buffer.readUint32();

    mStamp = buffer.readUint32();
    mHostName = buffer.readString();
    mUID = buffer.readUint32();
    mGID = buffer.readUint32();

    int length = buffer.readUint32();
    mAuxGIDs = new int[length];
    for (int i = 0; i < length; i++) {
      mAuxGIDs[i] = buffer.readUint32();
    }
  }

  public void setAuxGIDs(int[] auxGIDs) {
    this.mAuxGIDs = auxGIDs;
  }

  public void setGID(int gid) {
    this.mGID = gid;
  }

  public void setUID(int uid) {
    this.mUID = uid;
  }

  @Override
  public void write(RPCBuffer buffer) {

    int offset = buffer.position();

    buffer.writeUint32(Integer.MAX_VALUE);

    buffer.writeUint32(mStamp);
    buffer.writeString(mHostName);
    buffer.writeUint32(mUID);
    buffer.writeUint32(mGID);
    if((mAuxGIDs == null) || (mAuxGIDs.length == 0)) {
      buffer.writeUint32(0);
    } else {
      buffer.writeUint32(mAuxGIDs.length);
      for (int i = 0; i < mAuxGIDs.length; i++) {
        buffer.writeUint32(mAuxGIDs[i]);
      }
    }

    mCredentialsLength = buffer.position() - offset - Bytes.SIZEOF_INT;  // do not include length

    buffer.putInt(offset, mCredentialsLength);
  }
}
