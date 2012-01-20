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
package com.cloudera.hadoop.hdfs.nfs.rpc;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import org.apache.log4j.Logger;

/**
 * Represents fields common to both RPCResponse and RPCRequest
 */
public abstract class RPCPacket implements MessageBase {
  protected static final Logger LOGGER = Logger.getLogger(RPCPacket.class);
  
  protected int mXid, mMessageType, mRpcVersion, mProgram, mProgramVersion, mProcedure;
  
  public void write(RPCBuffer buffer) {
    buffer.writeInt(Integer.MAX_VALUE); // save space
    buffer.writeInt(mXid);
    buffer.writeInt(mMessageType);
  }
  public void read(RPCBuffer buffer) {
    this.mXid = buffer.readInt();
    this.mMessageType = buffer.readInt();    
  }
   
  public int getXid() {
    return mXid;
  }
  public void setXid(int xid) {
    this.mXid = xid;
  }
  public int getMessageType() {
    return mMessageType;
  }
  public void setMessageType(int messageType) {
    this.mMessageType = messageType;
  }
  public int getRpcVersion() {
    return mRpcVersion;
  }
  public void setRpcVersion(int rpcVersion) {
    this.mRpcVersion = rpcVersion;
  }
  public int getProgram() {
    return mProgram;
  }
  public void setProgram(int program) {
    this.mProgram = program;
  }
  public int getProgramVersion() {
    return mProgramVersion;
  }
  public void setProgramVersion(int programVersion) {
    this.mProgramVersion = programVersion;
  }
  public int getProcedure() {
    return mProcedure;
  }
  public void setProcedure(int procedure) {
    this.mProcedure = procedure;
  }

  @Override
  public String toString() {
    return "RPCPacket [xid="
        + mXid + ", messageType=" + mMessageType + ", rpcVersion=" + mRpcVersion
        + ", program=" + mProgram + ", programVersion=" + mProgramVersion
        + ", procedure=" + mProcedure + "]";
  }
}
