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

import junit.framework.Assert;

import org.junit.Test;

public class TestClientHostsMatcher {

  private final String address1 = "192.168.0.1";
  private final String address2 = "10.0.0.1";
  private final String hostname1 = "a.b.com";
  private final String hostname2 = "a.b.org";
  
  @Test
  public void testWildcard() {
    ClientHostsMatcher matcher = new ClientHostsMatcher("*");
    Assert.assertTrue(matcher.isIncluded(address1, hostname1));
  }
  
  @Test
  public void testExactAddress() {
    ClientHostsMatcher matcher = new ClientHostsMatcher(address1);
    Assert.assertTrue(matcher.isIncluded(address1, hostname1));
    Assert.assertFalse(matcher.isIncluded(address2, hostname1));
  }
  
  @Test
  public void testExactHost() {
    ClientHostsMatcher matcher = new ClientHostsMatcher(hostname1);
    Assert.assertTrue(matcher.isIncluded(address1, hostname1));
  }
  
  @Test
  public void testCidrShort() {
    ClientHostsMatcher matcher = new ClientHostsMatcher("192.168.0.0/22");
    Assert.assertTrue(matcher.isIncluded(address1, hostname1));
    Assert.assertFalse(matcher.isIncluded(address2, hostname1));
  }
  
  @Test
  public void testCidrLong() {
    ClientHostsMatcher matcher = new ClientHostsMatcher("192.168.0.0/255.255.252.0");
    Assert.assertTrue(matcher.isIncluded(address1, hostname1));
    Assert.assertFalse(matcher.isIncluded(address2, hostname1));
  }
  
  @Test
  public void testRegexIP() {
    ClientHostsMatcher matcher = new ClientHostsMatcher("192.168.0.[0-9]+");
    Assert.assertTrue(matcher.isIncluded(address1, hostname1));
    Assert.assertFalse(matcher.isIncluded(address2, hostname1));
  }
  @Test
  public void testRegexHost() {
    ClientHostsMatcher matcher = new ClientHostsMatcher("[a-z]+.b.com");
    Assert.assertTrue(matcher.isIncluded(address1, hostname1));
    Assert.assertFalse(matcher.isIncluded(address1, hostname2));
  }
}
