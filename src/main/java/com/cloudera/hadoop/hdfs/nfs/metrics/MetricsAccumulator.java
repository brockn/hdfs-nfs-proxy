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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.metrics.MetricConstants.Metric;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MetricsAccumulator {
  private static class MetricsLoggerTask extends Thread {
    private final MetricsAccumulator mMetrics;
    private final long mSleepIntervalMS;
    public MetricsLoggerTask(MetricsAccumulator metrics, long sleepIntervalMS) {
      mMetrics = metrics;
      mSleepIntervalMS = sleepIntervalMS;
    }
    @Override
    public void run() {
      try {
        while (true) {
          Thread.sleep(mSleepIntervalMS);
          mMetrics.publish();
        }
      } catch (Exception e) {
        LOGGER.error("Metrics Printer Quitting", e);
      }
    }
  }

  protected static final Logger LOGGER = Logger.getLogger(MetricsAccumulator.class);
  private final Map<String, AtomicLong> mAdhocMetrics = Maps.newHashMap();
  private final Map<Metric, AtomicLong> mMetrics = Maps.newHashMap();
  private final MetricsLoggerTask mMetricsPrinter;

  private final MetricPublisher mMetricPublisher;
  public MetricsAccumulator(MetricPublisher metricPublisher, long sleepIntervalMS) {
    mMetricPublisher = metricPublisher;
    mMetricsPrinter = new MetricsLoggerTask(this, sleepIntervalMS);
    mMetricsPrinter.setName("MetricsPrinter");
    mMetricsPrinter.setDaemon(true);
    mMetricsPrinter.start();
  }
  /**
   * Get a specific metric value
   */
  public synchronized long getMetric(Metric metric) {
    AtomicLong counter = mMetrics.get(metric);
    if (counter == null) {
      counter = new AtomicLong(0);
      mMetrics.put(metric, counter);
    }
    return counter.get();
  }
  /**
   * Get adhoc metric value, returning 0 if it does not exist
   */
  public synchronized long getMetric(String name) {
    name = name.toUpperCase();
    AtomicLong counter = mAdhocMetrics.get(name);
    if (counter == null) {
      counter = new AtomicLong(0);
      mAdhocMetrics.put(name, counter);
    }
    return counter.get();
  }
  /**
   * Increment a specific metric
   */
  public synchronized void incrementMetric(Metric metric, long count) {
    AtomicLong counter = mMetrics.get(metric);
    if (counter == null) {
      counter = new AtomicLong(0);
      mMetrics.put(metric, counter);
    }
    counter.addAndGet(count);
  }

  /**
   * Increment an adoc metric
   */
  public synchronized void incrementMetric(String name, long count) {
    name = name.toUpperCase();
    AtomicLong counter = mAdhocMetrics.get(name);
    if(counter == null) {
      counter = new AtomicLong(0);
      mAdhocMetrics.put(name, counter);
    }
    counter.addAndGet(count);
  }

  private synchronized void publish() {
    Map<String, Long> values = Maps.newTreeMap();
    for (String key : mAdhocMetrics.keySet()) {
      AtomicLong counter = mAdhocMetrics.get(key);
      values.put(key, counter.getAndSet(0L));
    }
    Set<Metric> metricsWithDivisors = Sets.newHashSet();
    for (Metric key : mMetrics.keySet()) {
      AtomicLong counter = mMetrics.get(key);
      String name = key.name();
      values.put(name, counter.getAndSet(0L));
      if(key.hasDivisor()) {
        metricsWithDivisors.add(key);
      }
    }
    for(Metric key : metricsWithDivisors) {
      Long counter = values.get(key.name());
      Long divisor = values.get(key.getDivisor());
      if((divisor != null) && (divisor > 0L)) {
        values.put(key.name() + "_AVG", (counter / divisor));
      }
    }
    mMetricPublisher.publish(values);
  }
}
