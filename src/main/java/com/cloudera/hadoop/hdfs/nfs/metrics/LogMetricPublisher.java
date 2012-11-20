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
package com.cloudera.hadoop.hdfs.nfs.metrics;

import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;

public class LogMetricPublisher implements MetricPublisher {
  private final Logger logger;
  private long lastPublish;
  public LogMetricPublisher(Logger logger) {
    this.logger = logger;
    this.lastPublish = System.currentTimeMillis();
  }
  @Override
  public void publish(Map<String, Long> values) {
    long interval = System.currentTimeMillis() - lastPublish;
    lastPublish = System.currentTimeMillis();
    interval /= 1000L;
    if(interval == 0) {
      interval++;
    }
    for (Map.Entry<String, Long> entry : values.entrySet()) {
      String name = entry.getKey();
      Long value = entry.getValue();
      if (name.contains("_BYTES_")) {
        logger.info("Metric: " + name + " = "
            + Bytes.toHuman(value / interval) + "/sec");
      } else {
        logger.info("Metric: " + name + " = " + value);
      }
    }

  }
}
