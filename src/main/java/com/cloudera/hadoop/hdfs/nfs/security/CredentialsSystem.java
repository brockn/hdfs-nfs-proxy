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

import com.cloudera.hadoop.hdfs.nfs.nfs4.UserIDMapper;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import org.apache.log4j.Logger;

/**
 * Implementation of RPC AUTH_SYS
 */
public class CredentialsSystem extends Credentials implements AuthenticatedCredentials {

    protected static final Logger LOGGER = Logger.getLogger(CredentialsSystem.class);
    protected int mUID, mGID;
    protected int[] mAuxGIDs;
    protected String mHostName;
    protected int mStamp;

    public CredentialsSystem() {
        super();
        this.mCredentialsLength = 0;
        this.mVerifierFlavor = RPC_VERIFIER_NULL;
        this.mVeriferLength = 0;
        this.mHostName = HOSTNAME;
    }

    @Override
    public void read(RPCBuffer buffer) {
        mCredentialsLength = buffer.readUint32();

        mStamp = buffer.readUint32();
        mHostName = buffer.readString();
        mUID = buffer.readUint32();
        mGID = buffer.readUint32();

        int length = buffer.readUint32();
        mAuxGIDs = new int[length];
        for (int i = 0; i < length; i++) {
            mAuxGIDs[i] = buffer.readUint32();
        }

        mVerifierFlavor = buffer.readInt();
        if (mVerifierFlavor != RPC_VERIFIER_NULL) {
            throw new RuntimeException("Verifer not accepted " + mVerifierFlavor);
        }
        mVeriferLength = buffer.readInt();
        if (mVeriferLength > RPC_OPAQUE_AUTH_MAX) {
            throw new RuntimeException("veriferLength too large " + mVeriferLength);
        }
        buffer.skip(mVeriferLength);
    }

    @Override
    public void write(RPCBuffer buffer) {

        int offset = buffer.position();

        buffer.writeUint32(Integer.MAX_VALUE);

        buffer.writeUint32(mStamp);
        buffer.writeString(mHostName);
        buffer.writeUint32(mUID);
        buffer.writeUint32(mGID);
        if (mAuxGIDs == null || mAuxGIDs.length == 0) {
            buffer.writeUint32(0);
        } else {
            buffer.writeUint32(mAuxGIDs.length);
            for (int i = 0; i < mAuxGIDs.length; i++) {
                buffer.writeUint32(mAuxGIDs[i]);
            }
        }

        mCredentialsLength = buffer.position() - offset;

        buffer.putInt(offset, mCredentialsLength);

        if (mVerifierFlavor != RPC_VERIFIER_NULL) {
            throw new RuntimeException("Verifer not accepted " + mVerifierFlavor);
        }
        buffer.writeInt(mVerifierFlavor);
        buffer.writeInt(mVeriferLength);
    }

    @Override
    public int getCredentialsFlavor() {
        return RPC_AUTH_UNIX;
    }

    public String getUsername(Configuration conf) throws Exception {
        UserIDMapper mapper = UserIDMapper.get(conf);
        String user = mapper.getUserForUID(mUID, null);
        if (user == null) {
            throw new Exception("Could not map " + mUID + " to user");
        }
        return user;
    }

    @Override
    public int getUID() {
        return mUID;
    }

    public void setUID(int uid) {
        this.mUID = uid;
    }

    @Override
    public int getGID() {
        return mGID;
    }

    public void setGID(int gid) {
        this.mGID = gid;
    }

    public int[] getAuxGIDs() {
        return mAuxGIDs;
    }

    public void setAuxGIDs(int[] auxGIDs) {
        this.mAuxGIDs = auxGIDs;
    }
}
