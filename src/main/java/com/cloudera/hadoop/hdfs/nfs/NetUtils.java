/**
 * Copyright 2012 The Apache Software Foundation
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

import java.net.InetAddress;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

public class NetUtils {
  protected static final Logger LOGGER = Logger.getLogger(NetUtils.class);
  private static final Random RANDOM = new Random();
  
  public static String getDomain(Configuration conf, InetAddress address) {
    String override = conf.get(NFS_OWNER_DOMAIN);
    if(override != null) {
      return override;
    }
    String host = address.getCanonicalHostName();
    if(address.isLoopbackAddress() &&
        address.getHostAddress().equals(address.getHostName())) {
      // loopback does not resolve
      return "localdomain";
    }
    int pos;
    if((pos = host.indexOf('.')) > 0 && pos < host.length()) {
      return host.substring(pos + 1);
    }
    LOGGER.error(Joiner.on("\n").join(
        "Unable to find the domain the server is running on. Please report.",
        "canonicalHostName = " + host, 
        "hostname = " + address.getHostName(),
        "isLoopback = " + address.isLoopbackAddress(),
        "hostAdddress = " + address.getHostAddress()));
    return "unknown";
  }

  
  public static synchronized int nextRandomInt() {
    return RANDOM.nextInt();
  }
}
