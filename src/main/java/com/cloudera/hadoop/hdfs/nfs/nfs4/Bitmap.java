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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.util.BitSet;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

/*
 * TODO fix this. I know this can be done cleaner.
 */

public class Bitmap implements MessageBase {
  protected static final Logger LOGGER = Logger.getLogger(Bitmap.class);

  protected static final int DEFAULT_NUM_BITS = 64;
  protected BitSet mMask = new BitSet(DEFAULT_NUM_BITS);
  @Override
  public void read(RPCBuffer buffer) {
    mMask = new BitSet(DEFAULT_NUM_BITS);
    int size = buffer.readUint32();
    for (int i = 0; i < size; i++) {
      int bitIndex = i * 32;
      int target = buffer.readInt();
      while(target != 0) {
        if((target & 0x01) != 0) {
          mMask.set(bitIndex);
        }
        target = target >>> 1;
        bitIndex++;
      }
    }
  }

  @Override
  public void write(RPCBuffer buffer) {
    if(mMask == null) {
      mMask = new BitSet(DEFAULT_NUM_BITS);
    }
    int bits = mMask.size();
    // all the written to an array of 32 bit integers
    int size = bits % 32 == 0 ? bits / 32 : (bits / 32) + 1;
    buffer.writeUint32(size);
    for (int i = 0; i < size; i++) {
      int target = 0;
      int startOffset = i * 32, endOffset = (i+1) * 32;
      for (int bitOffset = endOffset; bitOffset >= startOffset; bitOffset--) {
        if(mMask.get(bitOffset)) {
          target |= 0x01;
        }
        if(bitOffset != startOffset) {
          target = target << 1;
        }
      }
      buffer.writeInt(target);
    }
  }

  public boolean isEmpty() {
    return mMask.isEmpty();
  }
  public int size() {
    return mMask.size();
  }
  public void set(int bitIndex) {
    mMask.set(bitIndex);
  }

  public boolean isSet(int bitIndex) {
    return mMask.get(bitIndex);
  }

  public BitSet getMask() {
    return mMask;
  }

  @Override
  public String toString() {
    return String.valueOf(mMask);
  }
  public static void main(String[] args) {
    RPCBuffer buffer = new RPCBuffer();
    buffer.writeUint32(2);
    buffer.writeUint32(0);
    buffer.writeUint32(2);
    buffer.flip();
    Bitmap map = new Bitmap();
    map.read(buffer);
    System.out.println(map);
  }
}
