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

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class TestRPCBuffer {

  @Test
  public void testSmallString() throws Exception {
    RPCBuffer buffer = new RPCBuffer();
    buffer.writeString("/");
    buffer.writeBoolean(true);
    int pos = buffer.position();
    buffer.flip();
    assertTrue(buffer.length() == pos);
    assertTrue(buffer.position() == 0);
    String s;
    
    s = buffer.readString();
    assertTrue("s is '" + s + "'", s.equals("/"));
    assertTrue(buffer.readBoolean());

  }

  @Test
  public void testExpand() throws Exception {
    RPCBuffer buffer = new RPCBuffer(1);
    buffer.writeBoolean(true);
    buffer.writeString("brock");
    buffer.flip();
    assertTrue(buffer.readBoolean());
    String s = buffer.readString();
    assertEquals("brock", s);   
  }
  
  @Test
  public void testLargeString() throws Exception {
    StringBuilder stringBuffer = new StringBuilder();
    while(stringBuffer.length() < 512) {
      stringBuffer.append("brock,");
    }
    String s = stringBuffer.toString();
    RPCBuffer buffer = new RPCBuffer();
    buffer.writeString(s);
    buffer.writeBoolean(true);
    buffer.writeString(s);
    buffer.writeBoolean(false);
    int i = 0;
    while(i++ < 1000) {
      buffer.writeLong((long)i);
    }
    int pos = buffer.position();
    buffer.flip();
    assertTrue(buffer.length() == pos);
    assertTrue(buffer.position() == 0);
    String x;
    
    
    x = buffer.readString();
    assertEquals(s, x);
    assertTrue(buffer.readBoolean());
    x = buffer.readString();
    assertEquals(s, x);
    assertTrue(!buffer.readBoolean());

  }
  
  @Test
  public void testUnsigned() throws Exception {
    int sampleUnsigned32 = Integer.MAX_VALUE;
    long sampleUnsigned64 = Long.MAX_VALUE;
    RPCBuffer write = new RPCBuffer();
    try {
      write.writeUint32(Integer.MAX_VALUE + 1);
      fail("Expected NumberFormatException due to writing more than signed int");
    } catch (NumberFormatException e) {
      
    }
    try {
      write.writeUint32(-1);
      fail("Expected NumberFormatException due to writing negative");
    } catch (NumberFormatException e) {
      // expected
    }
    try {
      write.writeUint64(-1);
      fail("Expected NumberFormatException due to writing negative");
    } catch (NumberFormatException e) {
      // expected
    }
    write.writeUint32(sampleUnsigned32);
    write.writeUint64(sampleUnsigned64);

    write.flip();
    
    long unsignedInt = write.readUint32();
    assertTrue("Unexpected value " + unsignedInt, unsignedInt == sampleUnsigned32);
    long unsignedLong = write.readUint64();
    assertTrue("Unexpected value " + unsignedLong, unsignedLong == sampleUnsigned64);
  }
  @Test
  public void testBasic() throws Exception {
    String sampleString = "brock";
    byte[] sampleBytes = sampleString.getBytes();
    int sampleInt = Integer.MAX_VALUE;
    boolean sampleBool = true;
    RPCBuffer write = new RPCBuffer();
    write.writeString(sampleString);
    write.writeInt(sampleInt);
    write.writeInt(sampleBytes.length);
    write.writeBytes(sampleBytes, 0, sampleBytes.length);
    write.writeBoolean(sampleBool);
    
    write.flip();
    
    String s = write.readString();
    assertEquals(sampleString, s);
    int i = write.readInt();
    assertTrue("Int " + i + " is unxpected", 
        sampleInt == i);
    byte[] bytes = write.readBytes();
    assertTrue("Unxpected bytes array", Arrays.equals(sampleBytes, bytes));
    boolean bool = write.readBoolean();
    assertTrue("Expected " + sampleBool, 
        bool == sampleBool);
    
  }
}
