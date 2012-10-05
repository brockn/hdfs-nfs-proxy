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
package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static org.junit.Assert.fail;

import java.util.Set;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Identifiable;

public class TestAttribute {

  @Test
  public void testIdentifiers() throws Exception {
    Set<Integer> ids = Attribute.attributes.keySet();
    if(ids.isEmpty()) {
      fail("No attributes");
    }
    for(Integer id : ids) {
      Identifiable attribute = Attribute.attributes.get(id).clazz.newInstance();
      if(attribute.getID() != id) {
        fail(attribute.getClass().getName() + " has id " + attribute.getID() + " and not " + id);
      }
    }
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testNotSupported() {
    Attribute.checkSupported(Integer.MIN_VALUE);
  }
}
