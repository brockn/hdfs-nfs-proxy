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
package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import static org.mockito.Mockito.*;

import java.io.Closeable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData12;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.google.common.base.Charsets;

public class TestOpenResource {

  private StateID stateID;
  private Closeable closeable;
  private OpenResource<Closeable> res;
  
  @Before
  public void setup() throws Exception {
    stateID = new StateID();
    OpaqueData12 opaque = new OpaqueData12();
    opaque.setData("1".getBytes(Charsets.UTF_8));
    stateID.setData(opaque);
    closeable = mock(Closeable.class);
    res = new OpenResource<Closeable>(stateID, closeable);

  }
  @Test
  public void testClose() throws Exception {
    res.close();
    verify(closeable).close();
  }
  @Test
  public void testGet() throws Exception {
    Assert.assertSame(closeable, res.get());
  }
  @Test
  public void testIsOwnedBy() throws Exception {
    Assert.assertTrue(res.isOwnedBy(stateID));
    Assert.assertFalse(res.isOwnedBy(new StateID()));
  }
  @Test
  public void testSettersGettters() throws Exception {
    Assert.assertFalse(res.isConfirmed());
    res.setConfirmed(true);
    Assert.assertTrue(res.isConfirmed());
    res.setConfirmed(false);
    Assert.assertFalse(res.isConfirmed());

    Assert.assertEquals(stateID, res.getStateID());
    
    int seq = stateID.getSeqID() + 1;
    res.setSequenceID(seq);
    Assert.assertEquals(seq, stateID.getSeqID());
    
    long ts = res.getTimestamp() + 1;
    res.setTimestamp(ts);
    Assert.assertEquals(ts, res.getTimestamp());
  }
}
