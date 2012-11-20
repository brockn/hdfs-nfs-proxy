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

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class InitializeResponse implements MessageBase {

  private static final byte[] EMPTY = new byte[0];
  private byte[] contextID = EMPTY;
  private int majorErrorCode;
  private int minorErrorCode;
  private int sequenceWindow;
  private byte[] token = EMPTY;

  @Override
  public void read(RPCBuffer buffer) {
    contextID = buffer.readBytes();
    majorErrorCode = buffer.readInt();
    minorErrorCode = buffer.readInt();
    sequenceWindow = buffer.readUint32();
    token = buffer.readBytes();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(contextID.length);
    buffer.writeBytes(contextID);
    buffer.writeInt(majorErrorCode);
    buffer.writeInt(minorErrorCode);
    buffer.writeUint32(sequenceWindow);
    buffer.writeUint32(token.length);
    buffer.writeBytes(token);
  }

  public byte[] getContextID() {
    return contextID;
  }

  public void setContextID(byte[] contextID) {
    if(contextID == null) {
      contextID =  EMPTY;
    }
    this.contextID = contextID;
  }

  public int getMajorErrorCode() {
    return majorErrorCode;
  }

  public void setMajorErrorCode(int majorErrorCode) {
    this.majorErrorCode = majorErrorCode;
  }

  public int getMinorErrorCode() {
    return minorErrorCode;
  }

  public void setMinorErrorCode(int minorErrorCode) {
    this.minorErrorCode = minorErrorCode;
  }

  public int getSequenceWindow() {
    return sequenceWindow;
  }

  public void setSequenceWindow(int sequenceWindow) {
    this.sequenceWindow = sequenceWindow;
  }

  public byte[] getToken() {
    return token;
  }

  public void setToken(byte[] token) {
    if(token == null) {
      token =  EMPTY;
    }
    this.token = token;
  }
}