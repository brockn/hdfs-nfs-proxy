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
package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
/**
 * Implementation of RPC AUTH_GSS
 */
public class CredentialsGSS extends AuthenticatedCredentials {
  private int mVersion;
  private int mProcedure;
  private int mSequenceNum;
  private int mService;
  private OpaqueData mContext;

  public CredentialsGSS() {
    super();
    this.mCredentialsLength = 0;
  }

  public OpaqueData getContext() {
    return mContext;
  }

  @Override
  public int getFlavor() {
    return RPC_AUTH_GSS;
  }

  public int getProcedure() {
    return mProcedure;
  }

  public int getSequenceNum() {
    return mSequenceNum;
  }

  public int getService() {
    return mService;
  }

  public int getVersion() {
    return mVersion;
  }

  @Override
  public void read(RPCBuffer buffer) {
    mCredentialsLength = buffer.readUint32();
    mVersion = buffer.readUint32();
    if(mVersion != RPCSEC_GSS_VERSION) {
      throw new UnsupportedOperationException("Version " + mVersion);
    }
    mProcedure = buffer.readUint32();
    mSequenceNum = buffer.readUint32();
    mService = buffer.readUint32();
    int length = buffer.readUint32();
    mContext = new OpaqueData(length);
    mContext.read(buffer);
  }

  public void setContext(byte[] data) {
    OpaqueData opaqueData = new OpaqueData(data.length);
    opaqueData.setData(data);
    setContext(opaqueData);
  }

  public void setContext(OpaqueData context) {
    this.mContext = context;
  }

  public void setProcedure(int procedure) {
    this.mProcedure = procedure;
  }

  public void setSequenceNum(int sequenceNum) {
    this.mSequenceNum = sequenceNum;
  }

  public void setService(int service) {
    this.mService = service;
  }
  public void setVersion(int version) {
    this.mVersion = version;
  }

  @Override
  public void write(RPCBuffer buffer) {
    if(mVersion != RPCSEC_GSS_VERSION) {
      throw new UnsupportedOperationException("Version " + mVersion);
    }
    int offset = buffer.position();
    buffer.writeUint32(Integer.MAX_VALUE);

    buffer.writeUint32(mVersion);
    buffer.writeUint32(mProcedure);
    buffer.writeUint32(mSequenceNum);
    buffer.writeUint32(mService);
    buffer.writeUint32(mContext.getSize());
    mContext.write(buffer);

    mCredentialsLength = buffer.position() - offset - Bytes.SIZEOF_INT;  // do not include length

    buffer.putInt(offset, mCredentialsLength);
  }
}
