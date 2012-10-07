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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.google.common.collect.Maps;

public class Metrics {
  protected static final Logger LOGGER = Logger.getLogger(Metrics.class);

  private final Map<String, AtomicLong> mMetrics = Maps.newTreeMap();
  private final MetricsPrinter mMetricsPrinter;

  public Metrics() {
    mMetricsPrinter = new MetricsPrinter(mMetrics);
    mMetricsPrinter.setName("MetricsPrinter");
    mMetricsPrinter.setDaemon(true);
    mMetricsPrinter.start();
  }
  /**
   * Create or increment a named counter
   */
  public void incrementMetric(String name, long count) {
    name = name.toUpperCase();
    AtomicLong counter;
    synchronized (mMetrics) {
      counter = mMetrics.get(name);
      if (counter == null) {
        counter = new AtomicLong(0);
        mMetrics.put(name, counter);
      }
    }
    counter.addAndGet(count);
  }
  
  public long getMetric(String name) {
    name = name.toUpperCase();
    AtomicLong counter;
    synchronized (mMetrics) {
      counter = mMetrics.get(name);
      if (counter == null) {
        counter = new AtomicLong(0);
        mMetrics.put(name, counter);
      }
    }
    return counter.get();
  }
  
  protected static class MetricsPrinter extends Thread {

    Map<String, AtomicLong> mMetrics;

    public MetricsPrinter(Map<String, AtomicLong> metrics) {
      mMetrics = metrics;
    }

    @Override
    public void run() {
      try {
        while (true) {
          Thread.sleep(60000L);
          synchronized (mMetrics) {
            LOGGER.info("Metrics Start");
            for (String key : mMetrics.keySet()) {
              AtomicLong counter = mMetrics.get(key);
              long value = counter.getAndSet(0);
              if (key.contains("_BYTES_")) {
                LOGGER.info("Metric: " + key + " = "
                    + Bytes.toHuman(value / 60L) + "/sec");
              } else {
                LOGGER.info("Metric: " + key + " = " + value);
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Metrics Printer Quitting", e);
      }
    }
  }
}
