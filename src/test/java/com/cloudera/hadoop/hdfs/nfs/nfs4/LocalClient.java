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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.ietf.jgss.GSSManager;

import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCTestUtil;
import com.cloudera.hadoop.hdfs.nfs.security.SecurityHandlerFactory;
import com.cloudera.hadoop.hdfs.nfs.security.SessionSecurityHandlerGSSFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

public class LocalClient extends BaseClient {

  protected NFS4Handler mServer;

  public LocalClient() throws NFS4Exception, IOException {
    Configuration conf = TestUtils.setupConf();
    SecurityHandlerFactory securityHandlerFactory = 
        new SecurityHandlerFactory(
        conf, new Supplier<GSSManager>() {
          @Override
          public GSSManager get() {
            return GSSManager.getInstance();
          }
        }, new SessionSecurityHandlerGSSFactory());
    mServer = new NFS4Handler(conf, securityHandlerFactory);
    initialize();
  }

  @Override
  protected CompoundResponse doMakeRequest(CompoundRequest request) {
    try {
      CompoundResponse response = mServer.process(RPCTestUtil.createRequest(), request, LOCALHOST, "test").get();
      return response;
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void shutdown() {
    try {
      mServer.shutdown();
    } catch (IOException e) {
    }
  }


}
