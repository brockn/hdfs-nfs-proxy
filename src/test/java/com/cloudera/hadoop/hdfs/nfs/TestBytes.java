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
package com.cloudera.hadoop.hdfs.nfs;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class TestBytes {

  private final byte[] a = {
      (byte)'a',
      (byte)'b',
      (byte)'c',
    };
  private final byte[] b = {
        (byte)'x',
        (byte)'y',
        (byte)'z',
      };

 
  @Test
  public void testAdd() {
    Assert.assertEquals("abcxyz", new String(Bytes.add(a, b), Charsets.UTF_8));
  }
  @Test
  public void testAsHex() {
    Assert.assertEquals("31 32 33 34 35", Bytes.asHex("12345".getBytes(Charsets.UTF_8)));
  }
  @Test
  public void testCompare() {
    Assert.assertTrue(Bytes.compareTo(a, b) < 0);
    Assert.assertTrue(Bytes.compareTo(b, a) > 0);
    Assert.assertTrue(Bytes.compareTo(a, a) == 0);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testExplainWrongLengthOrOffsetInt() {
    Bytes.toInt(new byte[]{}, 0, 0);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testExplainWrongLengthOrOffsetLong() {
    Bytes.toLong(new byte[]{}, 0, 0);
  }
  
  
  @Test
  public void testInteger() {
    int expected = Integer.MIN_VALUE;
    Assert.assertEquals(expected, Bytes.toInt(Bytes.toBytes(expected)));
  }
  @Test
  public void testLong() {
    long expected = Long.MIN_VALUE;
    Assert.assertEquals(expected, Bytes.toLong(Bytes.toBytes(expected)));
  }
  
  @Test
  public void testMergeSingleArray() {
    List<byte[]> buffers = Lists.newArrayList();
    buffers.add(a);
    byte[] actual = Bytes.merge(buffers);
    Assert.assertArrayEquals(a, actual);
    Assert.assertSame(a, actual);
  }
  @Test
  public void testMergeTwoArrays() {
    List<byte[]> buffers = Lists.newArrayList();
    buffers.add(a);
    buffers.add(b);
    byte[] actual = Bytes.merge(buffers);
    Assert.assertArrayEquals("abcxyz".getBytes(Charsets.UTF_8), actual);
  }
  @Test
  public void testToHuman() {
    Assert.assertEquals("512 bytes", Bytes.toHuman(512L));
    Assert.assertEquals("512.00 KB", Bytes.toHuman(512L * 1024L));
    Assert.assertEquals("512.00 MB", Bytes.toHuman(512 * 1024L * 1024L));
    Assert.assertEquals("512.00 GB", Bytes.toHuman(512 * 1024L * 1024L * 1024L));
  }
  @Test
  public void testToString() {
    Pair<Object, Object> pair = Pair.of(null, null);
    pair.toString(); // does not throw NPE
  }
}
