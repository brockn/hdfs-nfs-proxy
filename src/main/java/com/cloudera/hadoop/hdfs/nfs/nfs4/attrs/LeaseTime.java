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
package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class LeaseTime extends Attribute {
  public LeaseTime() {
    super();
  }
  protected int mSeconds;
  @Override
  public void read(RPCBuffer buffer) {
    mSeconds = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mSeconds);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_LEASE_TIME;
  }

  public int getSeconds() {
    return mSeconds;
  }

  public void setSeconds(int seconds) {
    this.mSeconds = seconds;
  }


}
