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
package com.cloudera.hadoop.hdfs.nfs.metrics;

public class MetricConstants {
  private MetricConstants() {}
  public static enum Metric {
    GETATTR_DROPPED_ATTRS,
    HDFS_BYTES_WRITE,
    NFS_REQUESTS,
    NFS_COMPOUND_REQUESTS,
    COMPOUND_REQUEST_ELAPSED_TIME(NFS_COMPOUND_REQUESTS.name()),
    HDFS_BYTES_READ,
    NFS_SHORT_READS,
    NFS_RANDOM_READS,
    NFS_READDIR_ENTRIES,
    EXCEPTIONS,
    FILES_OPENED_WRITE,
    FILES_OPENED_READ,
    FILES_CLOSED,
    FILE_HANDLES_CREATED,
    RESTRANSMITS,
    CLIENT_BYTES_READ,
    CLIENT_BYTES_WROTE,
    REQUEST_ELAPSED_TIME;
    
    private final String divisor;
    private Metric() {
      this(null);
    }
    private Metric(String div) {
      this.divisor = div;
    }
    public boolean hasDivisor() {
      return getDivisor() != null;
    }
    public String getDivisor() {
      return divisor;
    }
  }
}
