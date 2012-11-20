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
import static com.cloudera.hadoop.hdfs.nfs.metrics.MetricConstants.Metric.*;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class TestMetricsAccumulator {

  private InMemoryMetricsPublisher publisher;
  private MetricsAccumulator accumulator;

  @Before
  public void setup() {
    publisher = new InMemoryMetricsPublisher();
    accumulator = new MetricsAccumulator(publisher, 1000L);
  }
  @Test
  public void testGetWithoutIncrement() {
    Assert.assertEquals(0, accumulator.getMetric("NFS_REQUESTS"));
    Assert.assertEquals(0, accumulator.getMetric(NFS_COMPOUND_REQUESTS));
  }

  @Test
  public void testIncrementEnum() {
    accumulator.incrementMetric(NFS_COMPOUND_REQUESTS, 1);
    Assert.assertEquals(1, accumulator.getMetric(NFS_COMPOUND_REQUESTS));
    accumulator.incrementMetric(NFS_COMPOUND_REQUESTS, 1);
    Assert.assertEquals(2, accumulator.getMetric(NFS_COMPOUND_REQUESTS));
  }
  @Test
  public void testIncrementString() {
    accumulator.incrementMetric("NFS_REQUESTS", 1);
    Assert.assertEquals(1, accumulator.getMetric("NFS_REQUESTS"));
    accumulator.incrementMetric("NFS_REQUESTS", 1);
    Assert.assertEquals(2, accumulator.getMetric("NFS_REQUESTS"));
  }
  @Test
  public void testPublish() throws Exception {
    accumulator.incrementMetric(NFS_COMPOUND_REQUESTS, 2);
    accumulator.incrementMetric(COMPOUND_REQUEST_ELAPSED_TIME, 5000L);
    accumulator.incrementMetric(NFS_REQUESTS, 1);
    accumulator.incrementMetric("NFS_COMPOUND_REQUESTS", 1);
    Thread.sleep(2000L);
    Map<String, Long> values = publisher.getValues();
    Assert.assertEquals(new Long(1L), values.get("NFS_REQUESTS"));
    Assert.assertEquals(new Long(2L), values.get("NFS_COMPOUND_REQUESTS"));
    Assert.assertEquals(new Long(5000L), values.get("COMPOUND_REQUEST_ELAPSED_TIME"));
    Assert.assertEquals(new Long(2500L), values.get("COMPOUND_REQUEST_ELAPSED_TIME_AVG"));
    Assert.assertEquals(new Long(1L), values.get("NFS_REQUESTS"));
  }
}
