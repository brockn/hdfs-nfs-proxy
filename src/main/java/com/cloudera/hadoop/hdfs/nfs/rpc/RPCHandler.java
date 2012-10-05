/**
 * Copyright 2012 The Apache Software Foundation
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
package com.cloudera.hadoop.hdfs.nfs.rpc;

import java.net.InetAddress;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.google.common.util.concurrent.ListenableFuture;


public abstract class RPCHandler<REQUEST extends MessageBase, RESPONSE extends MessageBase> {

  public abstract ListenableFuture<RESPONSE> process(final RPCRequest rpcRequest, final REQUEST request, final InetAddress clientAddress, final String sessionID);

  public abstract RESPONSE createResponse();

  public abstract REQUEST createRequest();

  public abstract void incrementMetric(String name, long count);


}
