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

import static com.cloudera.hadoop.hdfs.nfs.TestUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.LogUtils;
import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.FileID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Mode;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Size;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Type;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.ACCESSRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.GETATTRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.GETFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OperationRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.PUTROOTFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETCLIENTIDRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.ACCESSResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.GETATTRResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.GETFHResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OperationResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.PUTROOTFHResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETCLIENTIDResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestManual {
  protected static final Logger LOGGER = LoggerFactory.getLogger(TestManual.class);

  @Test
  public void testCallback() {
    Callback base = new Callback();
    base.setAddr("addr");
    base.setNetID("netid");
    base.setCallbackProgram(15);
    Callback copy = new Callback();
    copy(base, copy);
    deepEquals(base, copy);
  }
  
  @Test
  public void testClientID() {
    ClientID base = new ClientID();
    OpaqueData data = new OpaqueData(4);
    data.setData("blah".getBytes());
    base.setOpaqueID(data);
    OpaqueData8 verifer = new OpaqueData8();
    verifer.setData("aaaabbbb".getBytes());
    base.setVerifer(verifer);
    ClientID copy = new ClientID();
    copy(base, copy);
    deepEquals(base, copy);
  }
  
  @Test
  public void testClient() {
    ClientFactory clientFactory = new ClientFactory();
    ClientID clientID1 = new ClientID();
    OpaqueData opaqueData1 = new OpaqueData(4);
    opaqueData1.setData("blah".getBytes());
    clientID1.setOpaqueID(opaqueData1);
    OpaqueData8 verifer1 = new OpaqueData8();
    verifer1.setData("aaaabbbb".getBytes());
    clientID1.setVerifer(verifer1);
    Callback callback1 = new Callback();
    callback1.setAddr("addr");
    callback1.setNetID("netid");
    callback1.setCallbackProgram(15);
    
    ClientID clientID2 = new ClientID();
    Callback callback2 = new Callback();
    
    copy(clientID1, clientID2);
    copy(callback1, callback2);
    
    // copy leads to equal object
    deepEquals(clientID1, clientID2);
    deepEquals(callback1, callback2);
    
    // Client.get for same data leads to same objects
    Client client1 = clientFactory.createIfNotExist(clientID1);
    assertNotNull(client1);
    
    Client client2 = clientFactory.createIfNotExist(clientID2);
    assertNull(client2);
    client2 = clientFactory.get(clientID2.getOpaqueID());
    assertSame(client1, client2);
    
    // changing data leads to different object
    OpaqueData opaqueData2 = new OpaqueData(4);
    opaqueData2.setData("XXXX".getBytes());
    clientID2.setOpaqueID(opaqueData2);
    client2 = clientFactory.createIfNotExist(clientID2);
    assertNotSame(client1, client2);
  }
  @Test
  public void testSETCLIENTID() throws IOException {
    NFS4Handler server = new NFS4Handler(TestUtils.setupConf());
    CompoundRequest request = new CompoundRequest();
    request.setCredentials(TestUtils.newCredentials());
    SETCLIENTIDRequest setClientIDRequest = new SETCLIENTIDRequest();
    Callback callback = new Callback();
    callback.setAddr("addr");
    callback.setNetID("netid");
    setClientIDRequest.setCallback(callback);
    ClientID clientID = new ClientID();
    OpaqueData opaqueID = new OpaqueData(3);
    opaqueID.setData("abc".getBytes());
    clientID.setOpaqueID(opaqueID);
    OpaqueData8 verifer = new OpaqueData8();
    verifer.setData(Bytes.toBytes(5));
    clientID.setVerifer(verifer);
    setClientIDRequest.setClientID(clientID);
    List<OperationRequest> operations = Lists.newArrayList();
    operations.add(setClientIDRequest);
    request.setOperations(operations);
    CompoundResponse response = server.process(new RPCRequest(), request, LOCALHOST, "test");
    assertEquals(NFS4_OK, response.getStatus());
    SETCLIENTIDResponse setClientIDResponse = (SETCLIENTIDResponse)response.getOperations().get(0);
    assertEquals(NFS4_OK, setClientIDResponse.getStatus());
    assertTrue(setClientIDResponse.getClientID() >= 0);
    assertNotNull(setClientIDResponse.getVerifer());
  }
  
  
  @Test
  public void testBasicPUTROOTFH() throws IOException {
    NFS4Handler server = new NFS4Handler(TestUtils.setupConf());
    CompoundRequest request = new CompoundRequest();
    request.setCredentials(TestUtils.newCredentials());
    List<OperationRequest> operations = Lists.newArrayList();
    operations.add(new PUTROOTFHRequest());
    request.setOperations(operations);
    CompoundResponse response = server.process(new RPCRequest(), request, LOCALHOST, "test");
    assertTrue(response.getOperations().size() == 1);
    OperationResponse operationResponse = response.getOperations().get(0);
    assertTrue(operationResponse instanceof PUTROOTFHResponse);
    assertEquals(NFS4_OK, operationResponse.getStatus());
  }
  
  @Test
  public void testACCESS() throws IOException {
    NFS4Handler server = new NFS4Handler(TestUtils.setupConf());
    CompoundRequest request = new CompoundRequest();
    request.setCredentials(TestUtils.newCredentials());
    List<OperationRequest> operations = Lists.newArrayList();
    operations.add(new PUTROOTFHRequest());
    operations.add(new GETFHRequest());
    ACCESSRequest accesssRequest = new ACCESSRequest();
    accesssRequest.setAccess(NFS_ACCESS_READ);
    operations.add(accesssRequest);
    request.setOperations(operations);
    CompoundResponse response = server.process(new RPCRequest(), request, LOCALHOST, "test");
    assertEquals(NFS4_OK, response.getStatus());
    assertTrue(response.getOperations().size() == 3);
    ImmutableList<OperationResponse> operationResponses = response.getOperations();
    OperationResponse operationResponse;
    
    operationResponse = operationResponses.get(0);
    assertTrue(operationResponse instanceof PUTROOTFHResponse);
    assertEquals(NFS4_OK, operationResponse.getStatus());
    
    operationResponse = operationResponses.get(1);
    assertTrue(operationResponse instanceof GETFHResponse);
    assertEquals(NFS4_OK, operationResponse.getStatus());
    GETFHResponse getFHResponse = (GETFHResponse) operationResponse;
    assertNotNull(getFHResponse.getFileHandle());
    
    operationResponse = operationResponses.get(2);
    assertTrue(operationResponse instanceof ACCESSResponse);
    assertEquals(NFS4_OK, operationResponse.getStatus());
    ACCESSResponse accessResponse = (ACCESSResponse) operationResponse;
    assertEquals(NFS_ACCESS_READ, accessResponse.getAccess());
  }
  
  @Test
  public void testEndToEnd() throws IOException {
    NFS4Handler server = new NFS4Handler(TestUtils.setupConf());
    CompoundRequest compoundRequest = new CompoundRequest();
    compoundRequest.setCredentials(TestUtils.newCredentials());
    List<OperationRequest> operations = Lists.newArrayList();
    operations.add(new PUTROOTFHRequest());
    operations.add(new GETFHRequest());
    GETATTRRequest getAttrRequest = new GETATTRRequest();
    Bitmap requestAttrs = new Bitmap();    
    
    requestAttrs.set(NFS4_FATTR4_SUPPORTED_ATTRS);
    requestAttrs.set(NFS4_FATTR4_TYPE);
    requestAttrs.set(NFS4_FATTR4_FH_EXPIRE_TYPE);
    requestAttrs.set(NFS4_FATTR4_CHANGE);
    requestAttrs.set(NFS4_FATTR4_SIZE);
    requestAttrs.set(NFS4_FATTR4_LINK_SUPPORT);
    requestAttrs.set(NFS4_FATTR4_SYMLINK_SUPPORT);
    requestAttrs.set(NFS4_FATTR4_NAMED_ATTR);
    requestAttrs.set(NFS4_FATTR4_FSID);
    requestAttrs.set(NFS4_FATTR4_UNIQUE_HANDLES);
    requestAttrs.set(NFS4_FATTR4_LEASE_TIME);
    requestAttrs.set(NFS4_FATTR4_RDATTR_ERROR);
    requestAttrs.set(NFS4_FATTR4_ACL_SUPPORT);
    requestAttrs.set(NFS4_FATTR4_FILEHANDLE);
    requestAttrs.set(NFS4_FATTR4_CASE_INSENSITIVE);
    requestAttrs.set(NFS4_FATTR4_CASE_PRESERVING);
    requestAttrs.set(NFS4_FATTR4_CHOWN_RESTRICTED);
    requestAttrs.set(NFS4_FATTR4_FILEID);
    requestAttrs.set(NFS4_FATTR4_FILES_AVAIL);
    requestAttrs.set(NFS4_FATTR4_FILES_FREE);
    requestAttrs.set(NFS4_FATTR4_FILES_TOTAL);
    requestAttrs.set(NFS4_FATTR4_FS_LOCATIONS);
    requestAttrs.set(NFS4_FATTR4_HIDDEN);
    requestAttrs.set(NFS4_FATTR4_HOMOGENEOUS);
    requestAttrs.set(NFS4_FATTR4_MAXFILESIZE);
    requestAttrs.set(NFS4_FATTR4_MAXLINK);
    requestAttrs.set(NFS4_FATTR4_MAXNAME);
    requestAttrs.set(NFS4_FATTR4_MAXREAD);
    requestAttrs.set(NFS4_FATTR4_MAXWRITE);
    requestAttrs.set(NFS4_FATTR4_MIMETYPE);
    requestAttrs.set(NFS4_FATTR4_MODE);
    requestAttrs.set(NFS4_FATTR4_NO_TRUNC);
    requestAttrs.set(NFS4_FATTR4_NUMLINKS);
    requestAttrs.set(NFS4_FATTR4_OWNER);
    requestAttrs.set(NFS4_FATTR4_OWNER_GROUP);
    requestAttrs.set(NFS4_FATTR4_QUOTA_AVAIL_HARD);
    requestAttrs.set(NFS4_FATTR4_QUOTA_AVAIL_SOFT);
    requestAttrs.set(NFS4_FATTR4_QUOTA_USED);
    requestAttrs.set(NFS4_FATTR4_RAWDEV);
    requestAttrs.set(NFS4_FATTR4_SPACE_AVAIL);
    requestAttrs.set(NFS4_FATTR4_SPACE_FREE);
    requestAttrs.set(NFS4_FATTR4_SPACE_TOTAL);
    requestAttrs.set(NFS4_FATTR4_SPACE_USED);
    requestAttrs.set(NFS4_FATTR4_SYSTEM);
    requestAttrs.set(NFS4_FATTR4_TIME_ACCESS);
    requestAttrs.set(NFS4_FATTR4_TIME_BACKUP);
    requestAttrs.set(NFS4_FATTR4_TIME_CREATE);
    requestAttrs.set(NFS4_FATTR4_TIME_DELTA);
    requestAttrs.set(NFS4_FATTR4_TIME_METADATA);
    requestAttrs.set(NFS4_FATTR4_TIME_MODIFY);
    requestAttrs.set(NFS4_FATTR4_MOUNTED_ON_FILEID);

    
    getAttrRequest.setAttrs(requestAttrs);
    operations.add(getAttrRequest);
    compoundRequest.setOperations(operations);
    CompoundResponse response = server.process(new RPCRequest(), compoundRequest, LOCALHOST, "test");
    assertEquals(NFS4_OK, response.getStatus());
    
    assertTrue(response.getOperations().size() == 3);
    ImmutableList<OperationResponse> operationResponses = response.getOperations();
    OperationResponse operationResponse;
    
    operationResponse = operationResponses.get(0);
    assertTrue(operationResponse instanceof PUTROOTFHResponse);
    assertEquals(NFS4_OK, operationResponse.getStatus());
    
    operationResponse = operationResponses.get(1);
    assertTrue(operationResponse instanceof GETFHResponse);
    assertEquals(NFS4_OK, operationResponse.getStatus());
    GETATTRResponse attrResponse = (GETATTRResponse)operationResponses.get(2);
    assertEquals(NFS4_OK, attrResponse.getStatus());
    Map<Integer, Attribute> attrMap = Maps.newHashMap();
    for(Attribute attr : attrResponse.getAttrValues().values()) {
      attrMap.put(attr.getID(), serde(attr));
    }
    FileID fileId = (FileID)attrMap.get(NFS4_FATTR4_FILEID);
    Mode mode = (Mode)attrMap.get(NFS4_FATTR4_MODE);
    Type type = (Type)attrMap.get(NFS4_FATTR4_TYPE);
    assertEquals(NFS4_DIR, type.getType());
    assertTrue(fileId.getFileID() != 0);
    assertTrue(mode.getMode() != 0);
    assertNotNull(attrMap.get(NFS4_FATTR4_SIZE));
    assertTrue(attrMap.get(NFS4_FATTR4_SIZE) instanceof Size);
    assertNotNull(attrMap.get(NFS4_FATTR4_OWNER));
    assertNotNull(attrMap.get(NFS4_FATTR4_OWNER_GROUP));
    assertNotNull(attrMap.get(NFS4_FATTR4_TIME_MODIFY));
  }
  
  /**
   * Copies the message to byes and reads it back in again. 
   * Should execute both serialization and deserialization
   * code.
   * 
   * @param message
   * @return message
   */
  protected <T extends MessageBase> T serde(T message) {
    RPCBuffer buffer = new RPCBuffer();
    message.write(buffer);
    buffer.flip();
    try {
      message.read(buffer);
    } catch(RuntimeException x) {
      LOGGER.warn("Error reading buffer: " + LogUtils.dump(message), x);
      throw x;
    }
    return message;
  }
}
