/**
 * Copyright 2011 The Apache Software Foundation
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
package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.apache.hadoop.conf.Configuration;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import org.apache.log4j.Logger;

/**
 * Implementation of AUTH_NONE
 */
public class CredentialsNone extends Credentials {
    protected static final Logger LOGGER = Logger.getLogger(CredentialsNone.class);
    public CredentialsNone() {
        super();
        this.mCredentialsLength = 0;
    }
    @Override
    public void read(RPCBuffer buffer) {
        mCredentialsLength = buffer.readInt();
        if (mCredentialsLength != 0) {
            throw new RuntimeException("Length of " + this + " should be 0");
        }
    }
    @Override
    public void write(RPCBuffer buffer) {
        if (mCredentialsLength != 0) {
            throw new RuntimeException("Length of " + this + " should be 0");
        }
        buffer.writeInt(mCredentialsLength);
    }

    @Override
    public int getFlavor() {
        return RPC_AUTH_NULL;
    }

    public String getUsername(Configuration conf) throws Exception{
        return ANONYMOUS_USERNAME;
    }
}
