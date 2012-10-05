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

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPair {

  protected static final Logger LOGGER = Logger.getLogger(TestPair.class);

  @Before
  public void setup() {
    LOGGER.debug("setup");
  }

  @Test
  public void testToString() {
    Pair<Object, Object> pair = Pair.of(null, null);
    pair.toString(); // does not throw NPE
  }

  @Test
  public void testGetters() {
    Object left = new Object();
    Object right = new Object();
    Pair<Object, Object> pair = Pair.of(left, right);
    Assert.assertEquals(left, pair.getFirst());
    Assert.assertEquals(right, pair.getSecond());
  }
}
