/**
 * Copyright 2012 Cloudera Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RENEWRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.Client;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.ClientFactory;
import com.google.common.base.Charsets;

public class TestRENEWHandler extends TestBaseHandler {

  private RENEWHandler handler;
  private RENEWRequest request;

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();
    handler = new RENEWHandler();
    request = new RENEWRequest();
    ClientFactory clientFactory = new ClientFactory();
    when(hdfsState.getClientFactory()).thenReturn(clientFactory);
  }

  @Test
  public void testKnownClientID() throws Exception {
    ClientFactory clientFactory = hdfsState.getClientFactory();
    ClientID clientID = new ClientID();
    clientID.setOpaqueID(new OpaqueData(UUID.randomUUID().toString().getBytes(Charsets.UTF_8)));
    Client client = clientFactory.createIfNotExist(clientID);
    request.setClientID(client.getShorthandID());
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4_OK, response.getStatus());
  }
  @Test
  public void testUnknownClientID() throws Exception {
    request.setClientID(Long.MAX_VALUE);
    Status response = handler.handle(hdfsState, session, request);
    assertEquals(NFS4ERR_STALE_CLIENTID, response.getStatus());
  }
}
