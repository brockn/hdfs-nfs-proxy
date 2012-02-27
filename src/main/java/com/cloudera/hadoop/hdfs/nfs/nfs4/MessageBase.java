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

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

/**
 * Messages in our case are byte arrays. They need to
 * deconstructed (read) from left to right. Wrapper by
 * wrapper.
 * 
 * RPC.read() -> NFS.read() -> NFS Data.read()
 * 
 * As such, we use the read method as we processing
 * the message. Each layer down in the stack is responsible
 * for reading it's portion of the message.
 * 
 * Writing messages is similar. We need to write the left
 * most portion first and then continue right.
 * 
 * RPC.write() -> NFS.write() -> NFS Data.write()
 * 
 * Before sending the message we call
 * RPCBuffer.writeBufferSize to update the size of the
 * buffer we which is bytes 1-7 in our case.
 */
public interface MessageBase {
  public void read(RPCBuffer buffer);
  public void write(RPCBuffer buffer);
}
