package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.TestUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Callback;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Client;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientFactory;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
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
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class TestManual {

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
  public void testSETCLIENTID() {
    NFS4Handler server = new NFS4Handler();
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
    CompoundResponse response = server.process(new RPCRequest(), request, "localhost", "test");
    assertEquals(NFS4_OK, response.getStatus());
    SETCLIENTIDResponse setClientIDResponse = (SETCLIENTIDResponse)response.getOperations().get(0);
    assertEquals(NFS4_OK, setClientIDResponse.getStatus());
    assertTrue(setClientIDResponse.getClientID() >= 0);
    assertNotNull(setClientIDResponse.getVerifer());
  }
  
  @Test
  public void testBasicPUTROOTFH() {
    NFS4Handler server = new NFS4Handler();
    CompoundRequest request = new CompoundRequest();
    request.setCredentials(TestUtils.newCredentials());
    List<OperationRequest> operations = Lists.newArrayList();
    operations.add(new PUTROOTFHRequest());
    request.setOperations(operations);
    CompoundResponse response = server.process(new RPCRequest(), request, "localhost", "test");
    assertTrue(response.getOperations().size() == 1);
    OperationResponse operationResponse = response.getOperations().get(0);
    assertTrue(operationResponse instanceof PUTROOTFHResponse);
    assertEquals(NFS4_OK, operationResponse.getStatus());
  }
  
  @Test
  public void testACCESS() {
    NFS4Handler server = new NFS4Handler();
    CompoundRequest request = new CompoundRequest();
    request.setCredentials(TestUtils.newCredentials());
    List<OperationRequest> operations = Lists.newArrayList();
    operations.add(new PUTROOTFHRequest());
    operations.add(new GETFHRequest());
    ACCESSRequest accesssRequest = new ACCESSRequest();
    accesssRequest.setAccess(NFS_ACCESS_READ);
    operations.add(accesssRequest);
    request.setOperations(operations);
    CompoundResponse response = server.process(new RPCRequest(), request, "localhost", "test");
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
  public void testEndToEnd() {
    NFS4Handler server = new NFS4Handler();
    CompoundRequest compoundRequest = new CompoundRequest();
    compoundRequest.setCredentials(TestUtils.newCredentials());
    List<OperationRequest> operations = Lists.newArrayList();
    operations.add(new PUTROOTFHRequest());
    operations.add(new GETFHRequest());
    GETATTRRequest getAttrRequest = new GETATTRRequest();
    Bitmap requestAttrs = new Bitmap();
    requestAttrs.set(NFS4_FATTR4_TYPE);
    requestAttrs.set(NFS4_FATTR4_CHANGE);
    requestAttrs.set(NFS4_FATTR4_SIZE);
    requestAttrs.set(NFS4_FATTR4_FSID);
    requestAttrs.set(NFS4_FATTR4_FILEID);
    requestAttrs.set(NFS4_FATTR4_MODE);
    requestAttrs.set(NFS4_FATTR4_NUMLINKS);
    requestAttrs.set(NFS4_FATTR4_OWNER);
    requestAttrs.set(NFS4_FATTR4_OWNER_GROUP);
    requestAttrs.set(NFS4_FATTR4_RAWDEV);
    requestAttrs.set(NFS4_FATTR4_SPACE_USED);
    requestAttrs.set(NFS4_FATTR4_TIME_ACCESS);
    requestAttrs.set(NFS4_FATTR4_TIME_METADATA);
    requestAttrs.set(NFS4_FATTR4_TIME_MODIFY);
    getAttrRequest.setAttrs(requestAttrs);
    operations.add(getAttrRequest);
    compoundRequest.setOperations(operations);
    CompoundResponse response = server.process(new RPCRequest(), compoundRequest, "localhost", "test");
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
    ImmutableMap<Integer, Attribute> attrs = attrResponse.getAttrValues();
    FileID fileId = (FileID)attrs.get(NFS4_FATTR4_FILEID);
    Mode mode = (Mode)attrs.get(NFS4_FATTR4_MODE);
    Type type = (Type)attrs.get(NFS4_FATTR4_TYPE);
    assertEquals(NFS4_DIR, type.getType());
    assertTrue(fileId.getFileID() != 0);
    assertTrue(mode.getMode() != 0);
    assertNotNull(attrs.get(NFS4_FATTR4_SIZE));
    assertTrue(attrs.get(NFS4_FATTR4_SIZE) instanceof Size);
    assertNotNull(attrs.get(NFS4_FATTR4_OWNER));
    assertNotNull(attrs.get(NFS4_FATTR4_OWNER_GROUP));
    assertNotNull(attrs.get(NFS4_FATTR4_TIME_MODIFY));
  }
}
