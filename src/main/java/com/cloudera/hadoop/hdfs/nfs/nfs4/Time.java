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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class Time implements MessageBase {
  protected long mSeconds;
  protected int mNanoSeconds;
  public Time() {
    this(0, 0);
  }
  public Time(long ms) {
    this((ms / 1000L), (int)(ms % 1000L * 1000000L));
  }
  public Time(long seconds, int nanoSeconds) {
    this.mSeconds = seconds;
    this.mNanoSeconds = nanoSeconds;
  }
  @Override
  public void read(RPCBuffer buffer) {
    mSeconds = buffer.readUint64();
    mNanoSeconds = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mSeconds);
    buffer.writeUint32(mNanoSeconds);
  }
  public long getSeconds() {
    return mSeconds;
  }
  public void setSeconds(long mSeconds) {
    this.mSeconds = mSeconds;
  }
  public int getNanoSeconds() {
    return mNanoSeconds;
  }
  public void setNanoSeconds(int mNanoSeconds) {
    this.mNanoSeconds = mNanoSeconds;
  }

  public long toMilliseconds() {
    long time = mSeconds * 1000L;
    time += mNanoSeconds / 1000000L;
    return time;
  }
}
