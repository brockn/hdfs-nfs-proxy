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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Maps;

public class TestLogMetricPublisher {

  private LogMetricPublisher publisher;
  private Logger logger;
  private String result;

  @Before
  public void setup() {
    logger = mock(Logger.class);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        result = (String)args[0];
        return null;
      }
    }).when(logger).info(any());
    publisher = new LogMetricPublisher(logger);
  }

  @Test
  public void testPublish() {
    Map<String, Long> values = Maps.newHashMap();
    values.put("metric-1", 1L);
    publisher.publish(values);
    Assert.assertEquals("Metric: metric-1 = 1", result);

  }
  @Test
  public void testPublishBytes() throws Exception {
    Thread.sleep(1001L);
    Map<String, Long> values = Maps.newHashMap();
    values.put("NUM_BYTES_SENT", 1000L);
    publisher.publish(values);
    Assert.assertEquals("Metric: NUM_BYTES_SENT = 1000 bytes/sec", result);
  }
  @Test
  public void testPublishBytesZeroInterval() throws Exception {
    Map<String, Long> values = Maps.newHashMap();
    values.put("NUM_BYTES_SENT", 1000L);
    publisher.publish(values);
    Assert.assertEquals("Metric: NUM_BYTES_SENT = 1000 bytes/sec", result);
  }
}
