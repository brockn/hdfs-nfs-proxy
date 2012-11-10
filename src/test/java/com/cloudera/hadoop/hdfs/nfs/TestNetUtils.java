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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.mockito.Mockito.*;

import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNetUtils {
  protected static final Logger LOGGER = Logger.getLogger(TestNetUtils.class);
  private InetAddress address;
  private Configuration conf;
  private String hostname;
  private String domain;
  private String fqdn;
  private String ip;
  @Before
  public void setup() {
    conf = new Configuration();
    address = mock(InetAddress.class);
    hostname = "fortesting";
    domain = "apache.org";
    fqdn = hostname + "." + domain;
    ip = "10.0.0.1";
    when(address.getCanonicalHostName()).thenReturn(fqdn);  
    when(address.getHostName()).thenReturn(hostname);  
    when(address.getHostAddress()).thenReturn(ip);  
  }
  @Test
  public void testGetDomain() {
    Assert.assertEquals(domain, NetUtils.getDomain(conf, address));
  }
  
  @Test
  public void testGetDomainLocalhost() {
    when(address.isLoopbackAddress()).thenReturn(true);
    Assert.assertEquals(domain, NetUtils.getDomain(conf, address));
  }
  @Test
  public void testGetDomainLocalhostUnresolable() {
    when(address.isLoopbackAddress()).thenReturn(true);
    when(address.getHostName()).thenReturn(ip);  
    Assert.assertEquals("localdomain", NetUtils.getDomain(conf, address));
  }
  @Test
  public void testGetDomainNoDomain() {
    when(address.getCanonicalHostName()).thenReturn(hostname); 
    Assert.assertEquals("unknown", NetUtils.getDomain(conf, address));
  }
  @Test
  public void testGetDomainOnlyDomain() {
    when(address.getCanonicalHostName()).thenReturn("." + hostname); 
    Assert.assertEquals("unknown", NetUtils.getDomain(conf, address));
  }
  @Test
  public void testGetDomainOverride() {
    conf.set(NFS_OWNER_DOMAIN, domain);
    Assert.assertEquals(domain, NetUtils.getDomain(conf, address));
  }
}
