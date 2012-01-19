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
package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.security.Credentials;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsNone;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsSystem;
import org.apache.log4j.Logger;

/**
 * Represents an RPC Request as defined by the RPC RFC.
 */
public class RPCRequest extends RPCPacket {

    protected static final Logger LOGGER = Logger.getLogger(RPCRequest.class);
    protected int mCredentialsFlavor;
    protected Credentials mCredentials;

    public RPCRequest(int xid, int rpcVersion) {
        this.mXid = xid;

        this.mMessageType = RPC_MESSAGE_TYPE_CALL;
        this.mRpcVersion = rpcVersion;


    }

    public RPCRequest() {
    }

    @Override
    public void write(RPCBuffer buffer) {
        super.write(buffer);
        buffer.writeUint32(mRpcVersion);
        buffer.writeUint32(mProgram);
        buffer.writeUint32(mProgramVersion);
        buffer.writeUint32(mProcedure);

        buffer.writeInt(mCredentialsFlavor);
        mCredentials.write(buffer);
    }

    @Override
    public void read(RPCBuffer buffer) {
        super.read(buffer);
        mRpcVersion = buffer.readUint32();
        mProgram = buffer.readUint32();
        mProgramVersion = buffer.readUint32();
        mProcedure = buffer.readUint32();
        mCredentialsFlavor = buffer.readInt();
        if (mCredentialsFlavor == RPC_AUTH_NULL) {
            mCredentials = new CredentialsNone();
        } else if (mCredentialsFlavor == RPC_AUTH_UNIX) {
            mCredentials = new CredentialsSystem();
        } else {
            throw new UnsupportedOperationException("Unsupported Credentials Flavor " + mCredentialsFlavor);
        }
        mCredentials.read(buffer);
    }

    public Credentials getCredentials() {
        return mCredentials;
    }

    public void setCredentials(Credentials credentials) {
        this.mCredentials = credentials;
        if (mCredentials != null) {
            mCredentialsFlavor = mCredentials.getCredentialsFlavor();
        }
    }

    @Override
    public String toString() {
        return "RPCRequest [mCredentialsFlavor=" + mCredentialsFlavor
                + ", mCredentials=" + mCredentials + ", mXid=" + mXid
                + ", mMessageType=" + mMessageType + ", mRpcVersion=" + mRpcVersion
                + ", mProgram=" + mProgram + ", mProgramVersion=" + mProgramVersion
                + ", mProcedure=" + mProcedure + "]";
    }
}