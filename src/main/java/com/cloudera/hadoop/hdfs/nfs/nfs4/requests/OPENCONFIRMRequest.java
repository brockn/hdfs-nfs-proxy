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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class OPENCONFIRMRequest extends OperationRequest {
  protected StateID mStateID;
  protected int mSeqID;
  @Override
  public int getID() {
    return NFS4_OP_OPEN_CONFIRM;
  }

  public int getSeqID() {
    return mSeqID;
  }

  public StateID getStateID() {
    return mStateID;
  }

  @Override
  public void read(RPCBuffer buffer) {
    mStateID = new StateID();
    mStateID.read(buffer);
    mSeqID = buffer.readUint32();
  }

  public void setSeqID(int seqID) {
    this.mSeqID = seqID;
  }

  public void setStateID(StateID stateID) {
    this.mStateID = stateID;
  }

  @Override
  public void write(RPCBuffer buffer) {
    mStateID.write(buffer);
    buffer.writeUint32(mSeqID);
  }

}
