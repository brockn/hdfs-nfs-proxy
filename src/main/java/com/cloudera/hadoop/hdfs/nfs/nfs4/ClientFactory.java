/**
 * Copyright 2011 The Apache Software Foundation
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

import com.google.common.collect.Maps;
import java.util.Map;

/**
 * Class responsible for keeping a global map of shorthand
 * clientids to Client objects.
 */
public class ClientFactory {
  protected static final Map<Long, Client> mShortHandMap = Maps.newHashMap();
  protected static final Map<OpaqueData, Client> mClientMap = Maps.newHashMap();
  
  public synchronized Client get(OpaqueData clientID) {
    return mClientMap.get(clientID);
  }
  public synchronized Client createIfNotExist(ClientID clientID) {
    if(mClientMap.containsKey(clientID.getOpaqueID())) {
      return null;
    }
    Client client = new Client(clientID);
    mClientMap.put(clientID.getOpaqueID(), client);
    mShortHandMap.put(client.getShorthandID(), client);
    return client;
  }

  public synchronized Client getByShortHand(Long shortHand) {
    return mShortHandMap.get(shortHand);
  }
}
