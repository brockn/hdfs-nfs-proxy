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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.LogUtils;
import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CLOSERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.GETATTRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.GETFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.LOOKUPRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OPENCONFIRMRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OPENRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OperationRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.PUTFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.PUTROOTFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.READDIRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.READRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETCLIENTIDCONFIRMRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETCLIENTIDRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.WRITERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CLOSEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.GETATTRResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.GETFHResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.LOOKUPResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OPENCONFIRMResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OPENResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OperationResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.PUTFHResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.PUTROOTFHResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.READDIRResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.READResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETCLIENTIDCONFIRMResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETCLIENTIDResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.WRITEResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class BaseClient {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseClient.class);

  protected long clientID = 0;
  protected OpaqueData8 clientVerifer;
  protected Path ROOT = new Path("/");
  protected Map<FileHandle, StateID> mFileHandleStateID = Maps.newHashMap();
  protected Map<Path, FileHandle> mPathFileHandleMap = Maps.newHashMap();
  protected Map<FileHandle, Path> mFileHandlePathMap = Maps.newHashMap();
  protected Map<FileHandle, ImmutableMap<Integer, Attribute>> mFileHandleAttributeMap = Maps.newHashMap();

  
  
  protected void initialize() {
    CompoundRequest compoundRequest = newRequest();
    List<OperationRequest> operations = Lists.newArrayList();
    operations.add(new PUTROOTFHRequest());
    operations.add(new GETFHRequest());
    
    compoundRequest.setOperations(operations);

    List<OperationResponse> operationResponses = makeRequest(compoundRequest);
    
    getResponse(operationResponses.remove(0), PUTROOTFHResponse.class);
    
    GETFHResponse getFHResponse = getResponse(operationResponses.remove(0), GETFHResponse.class);
    FileHandle fileHandle = getFHResponse.getFileHandle();
    mPathFileHandleMap.put(ROOT, fileHandle);
    mFileHandlePathMap.put(fileHandle, ROOT);
    getAttrs(ROOT);
  }
  
  protected abstract CompoundResponse doMakeRequest(CompoundRequest request) throws IOException;

  
  public void shutdown() {
    
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
  /**
   * Makes request to server.
   * 
   * @param request
   * @return CompoundResponse
   */
  protected List<OperationResponse> makeRequest(CompoundRequest request) {
    request = serde(request);
    CompoundResponse response;
    try {
      response = doMakeRequest(request);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    assertEquals(NFS4_OK, response.getStatus());
    assertEquals(request.getOperations().size(), response.getOperations().size());
    response = serde(response);
    assertEquals(request.getOperations().size(), response.getOperations().size());
    return Lists.newArrayList(response.getOperations());
  }
  
  
  protected FileHandle lookup(Path path) {
    Path parent;
    LOGGER.info("Lookup on " + path);
    if(path.equals(ROOT)) {
      parent = path;
    } else {
      parent = path.getParent();      
    }
    FileHandle parentFileHandle = mPathFileHandleMap.get(parent);
    if(parentFileHandle == null) {
      parentFileHandle = lookup(parent);      
    }
    
    if(parent.equals(path)) {
      return parentFileHandle;
    }
    
    CompoundRequest compoundRequest = newRequest();
    List<OperationRequest> operations = Lists.newArrayList();
    PUTFHRequest putFhRequest = new PUTFHRequest();
    putFhRequest.setFileHandle(parentFileHandle);    
    operations.add(putFhRequest);
    LOOKUPRequest lookupRequest = new LOOKUPRequest();
    lookupRequest.setName(path.getName());    
    operations.add(lookupRequest);

    operations.add(new GETFHRequest());
    operations.add(newGETATTRRequest());
    
    compoundRequest.setOperations(operations);
    
    List<OperationResponse> operationResponses = makeRequest(compoundRequest);
    
    getResponse(operationResponses.remove(0), PUTFHResponse.class);
    getResponse(operationResponses.remove(0), LOOKUPResponse.class);
    GETFHResponse getFHResponse = getResponse(operationResponses.remove(0), GETFHResponse.class);
    FileHandle fileHandle = getFHResponse.getFileHandle();
    mPathFileHandleMap.put(path, fileHandle);
    mFileHandlePathMap.put(fileHandle, path);
    GETATTRResponse getAttrResponse = getResponse(operationResponses.remove(0), GETATTRResponse.class);
    mFileHandleAttributeMap.put(fileHandle, getAttrResponse.getAttrValues());
    return fileHandle;
  }
  
  protected static <T> T getResponse(OperationResponse operationResponse, Class<T> clazz) {
    assertEquals(clazz, operationResponse.getClass());
    assertEquals(NFS4_OK, operationResponse.getStatus());
    return clazz.cast(operationResponse);
  }
  public ImmutableList<Path> listPath(Path path) {
    
    FileHandle fileHandle = lookup(path);
    
    boolean eof = false;
    long cookie = 0;
    OpaqueData8 verifer = new OpaqueData8(0);
    List<Path> paths = Lists.newArrayList();
    while(!eof) {
      
      CompoundRequest compoundRequest = newRequest();
      List<OperationRequest> operations = Lists.newArrayList();
      PUTFHRequest putFhRequest = new PUTFHRequest();
      putFhRequest.setFileHandle(fileHandle);    
      operations.add(putFhRequest);

      
      READDIRRequest readdirRequest = new READDIRRequest();
      readdirRequest.setCookie(cookie);
      readdirRequest.setCookieVerifer(verifer);
      readdirRequest.setDirCount(1024);
      readdirRequest.setMaxCount(8192);
      readdirRequest.setAttrs(new Bitmap());
      operations.add(readdirRequest);

      
      compoundRequest.setOperations(operations);
      List<OperationResponse> operationResponses = makeRequest(compoundRequest);
      
      getResponse(operationResponses.remove(0), PUTFHResponse.class);
      READDIRResponse readDirResponse = getResponse(operationResponses.remove(0), READDIRResponse.class);
      verifer = readDirResponse.getCookieVerifer();
      eof = readDirResponse.getDirectoryList().isEOF();
      
      for(DirectoryEntry entry : readDirResponse.getDirectoryList().getDirEntries()) {
        cookie = entry.getCookie();
        paths.add(new Path(path, entry.getName()));
      }  
    }    
    return ImmutableList.copyOf(paths);
  }
  
  
  
  
  protected GETATTRRequest newGETATTRRequest() {
    GETATTRRequest getAttrRequest = new GETATTRRequest();
    Bitmap requestAttrs = new Bitmap();
    requestAttrs.set(NFS4_FATTR4_TYPE);
    requestAttrs.set(NFS4_FATTR4_CHANGE);
    requestAttrs.set(NFS4_FATTR4_SIZE);
    requestAttrs.set(NFS4_FATTR4_FSID);
    requestAttrs.set(NFS4_FATTR4_FILEID);
    requestAttrs.set(NFS4_FATTR4_MODE);
    requestAttrs.set(NFS4_FATTR4_OWNER);
    requestAttrs.set(NFS4_FATTR4_OWNER_GROUP);
    requestAttrs.set(NFS4_FATTR4_RAWDEV);
    requestAttrs.set(NFS4_FATTR4_SPACE_USED);
    requestAttrs.set(NFS4_FATTR4_TIME_ACCESS);
    requestAttrs.set(NFS4_FATTR4_TIME_METADATA);
    requestAttrs.set(NFS4_FATTR4_TIME_MODIFY);
    getAttrRequest.setAttrs(requestAttrs);
    return getAttrRequest;
  }
 
  protected CompoundRequest newRequest() {
    CompoundRequest compoundRequest = new CompoundRequest();
    compoundRequest.setCredentials(TestUtils.newCredentials());
    return compoundRequest;
  }

  protected void setClientIDIfUnset() {
    if(this.clientID == 0) {
      CompoundRequest compoundRequest = newRequest();
      List<OperationRequest> operations = Lists.newArrayList();
      SETCLIENTIDRequest setClientIDRequest = new SETCLIENTIDRequest();
      setClientIDRequest.setCallbackIdent(0);
      Callback callback = new Callback();
      callback.setAddr("addr");
      callback.setNetID("netid");
      callback.setCallbackProgram(0);
      setClientIDRequest.setCallback(callback);
      ClientID clientID = new ClientID();
      OpaqueData8 verifer = new OpaqueData8();
      verifer.setData("aaaabbbb".getBytes());
      clientID.setVerifer(verifer);
      OpaqueData id = new OpaqueData(10);
      id.setData("localhost".getBytes());
      clientID.setOpaqueID(id);
      setClientIDRequest.setClientID(clientID);
      operations.add(setClientIDRequest);

      compoundRequest.setOperations(operations);
      List<OperationResponse> operationResponses = makeRequest(compoundRequest);
      SETCLIENTIDResponse setClientIDResponse = getResponse(operationResponses.remove(0), SETCLIENTIDResponse.class);
      doConfirmClientID(setClientIDResponse.getClientID(), setClientIDResponse.getVerifer());
      this.clientID = setClientIDResponse.getClientID();
      this.clientVerifer = setClientIDResponse.getVerifer();      
    }
  }
  protected void doConfirmClientID(long clientID, OpaqueData8 verifer) {
    CompoundRequest compoundRequest = newRequest();
    List<OperationRequest> operations = Lists.newArrayList();
    SETCLIENTIDCONFIRMRequest confirmRequest = new SETCLIENTIDCONFIRMRequest();
    confirmRequest.setClientID(clientID);
    confirmRequest.setVerifer(verifer);    
    operations.add(confirmRequest);
    compoundRequest.setOperations(operations);
    List<OperationResponse> operationResponses = makeRequest(compoundRequest);
    getResponse(operationResponses.remove(0), SETCLIENTIDCONFIRMResponse.class);
  }
  protected StateID doOpen(FileHandle parentFileHandle, String name, 
      int access, int openType) {
    CompoundRequest compoundRequest = newRequest();
    List<OperationRequest> operations = Lists.newArrayList();
    PUTFHRequest putFhRequest = new PUTFHRequest();
    putFhRequest.setFileHandle(parentFileHandle);    
    operations.add(putFhRequest);
    OPENRequest openRequest = new OPENRequest();
    openRequest.setAccess(access);
    openRequest.setOpenType(openType);
    openRequest.setSeqID(0);
    openRequest.setClientID(clientID);
    openRequest.setClaimType(NFS4_CLAIM_NULL);
    OpaqueData owner = new OpaqueData(16);
    owner.setData(("clientid" + clientID).getBytes());
    openRequest.setOwner(owner);
    openRequest.setName(name);
    openRequest.setAttrs(new Bitmap());
    operations.add(openRequest);
    operations.add(new GETFHRequest());
    compoundRequest.setOperations(operations);    
    List<OperationResponse> operationResponses = makeRequest(compoundRequest);
    getResponse(operationResponses.remove(0), PUTFHResponse.class);
    OPENResponse openResponse = getResponse(operationResponses.remove(0), OPENResponse.class);
    GETFHResponse getFhHandle = getResponse(operationResponses.remove(0), GETFHResponse.class);
    FileHandle fileHandle = getFhHandle.getFileHandle();
    assertNotNull(fileHandle);
    StateID stateID = doOpenConfirm(fileHandle, openResponse.getStateID());
    Path parent = mFileHandlePathMap.get(parentFileHandle);
    Path path = new Path(parent, name);
    mPathFileHandleMap.put(path, fileHandle);
    mFileHandlePathMap.put(fileHandle, path);
    mFileHandleStateID.put(fileHandle, stateID);    
    return stateID;
     
  }
  protected StateID doOpenConfirm(FileHandle fileHandle, StateID stateID) {
    CompoundRequest compoundRequest = newRequest();
    List<OperationRequest> operations = Lists.newArrayList();
    PUTFHRequest putFhRequest = new PUTFHRequest();
    putFhRequest.setFileHandle(fileHandle);    
    operations.add(putFhRequest);
    OPENCONFIRMRequest openConfirmRequest = new OPENCONFIRMRequest();
    openConfirmRequest.setStateID(stateID);
    openConfirmRequest.setSeqID(stateID.getSeqID() + 1);
    operations.add(openConfirmRequest);
    compoundRequest.setOperations(operations);
    List<OperationResponse> operationResponses = makeRequest(compoundRequest);
    getResponse(operationResponses.remove(0), PUTFHResponse.class);
    OPENCONFIRMResponse openConfirmresponse = getResponse(operationResponses.remove(0), OPENCONFIRMResponse.class);
    return openConfirmresponse.getStateID();
  }
  
  public OutputStream forWrite(final Path path) throws Exception {   
    setClientIDIfUnset();
    
    final FileHandle parentFileHandle = checkNotNull(lookup(path.getParent()));
    final StateID stateID = checkNotNull(doOpen(parentFileHandle, path.getName(),
        NFS4_OPEN4_SHARE_ACCESS_WRITE, NFS4_OPEN4_CREATE));
    final FileHandle fileHandle = checkNotNull(mPathFileHandleMap.get(path));
    
    return new OutputStream() {
      protected long fileOffset = 0L;
      @Override
      public void write(int b) throws IOException {
        CompoundRequest compoundRequest = newRequest();
        List<OperationRequest> operations = Lists.newArrayList();
        PUTFHRequest putFhRequest = new PUTFHRequest();
        putFhRequest.setFileHandle(fileHandle);    
        operations.add(putFhRequest);

        WRITERequest writeRequest = new WRITERequest();
        byte[] data = new byte[1];
        data[0] = (byte)b;
        writeRequest.setData(data, 0, data.length);
        writeRequest.setOffset(fileOffset);
        writeRequest.setStable(NFS4_COMMIT_UNSTABLE4);
        writeRequest.setStateID(stateID);
        
        operations.add(writeRequest);
        
        compoundRequest.setOperations(operations);
        List<OperationResponse> operationResponses = makeRequest(compoundRequest);
        getResponse(operationResponses.remove(0), PUTFHResponse.class);
        
        WRITEResponse writeResponse = getResponse(operationResponses.remove(0), WRITEResponse.class);
        if(writeResponse.getCount() != data.length) {
          throw new IOException("Write failed: " + writeResponse.getCount());
        }
        fileOffset++;
      }
      
      @Override
      public void close() throws IOException {

        CompoundRequest compoundRequest = newRequest();
        List<OperationRequest> operations = Lists.newArrayList();
        PUTFHRequest putFhRequest = new PUTFHRequest();
        putFhRequest.setFileHandle(fileHandle);    
        operations.add(putFhRequest);

        CLOSERequest closeRequest = new CLOSERequest();
        closeRequest.setSeqID(stateID.getSeqID() + 1);
        closeRequest.setStateID(stateID);
        operations.add(closeRequest);
        
        compoundRequest.setOperations(operations);
        List<OperationResponse> operationResponses = makeRequest(compoundRequest);
        getResponse(operationResponses.remove(0), PUTFHResponse.class);

        CLOSEResponse closeResponse = getResponse(operationResponses.remove(0), CLOSEResponse.class);

        
        mFileHandleStateID.put(fileHandle, closeResponse.getStateID());
      }
    };
  }
  public InputStream forRead(final Path path, final int readSize) throws Exception {   
    setClientIDIfUnset();
    
    final FileHandle parentFileHandle = checkNotNull(lookup(path.getParent()));
    final StateID stateID = checkNotNull(doOpen(parentFileHandle, path.getName(), 
        NFS4_OPEN4_SHARE_ACCESS_READ, NFS4_OPEN4_NOCREATE));
    final FileHandle fileHandle = checkNotNull(mPathFileHandleMap.get(path));
    
    /*
     * Code below reads 1 byte per RPC. It's intended to test
     * to and not EVER be copied and used.
     */
    return new InputStream() {
      protected long fileOffset = 0L;
      protected byte[] buffer = new byte[readSize];
      protected int bufferOffset;
      protected int bufferLength;
      @Override
      public int read() throws IOException {
        
        if(bufferOffset < bufferLength) {
          fileOffset++;
          return buffer[bufferOffset++];
        }

        
        CompoundRequest compoundRequest = newRequest();
        List<OperationRequest> operations = Lists.newArrayList();
        PUTFHRequest putFhRequest = new PUTFHRequest();
        putFhRequest.setFileHandle(fileHandle);    
        operations.add(putFhRequest);

        READRequest readRequest = new READRequest();
        readRequest.setOffset(fileOffset);
        readRequest.setCount(buffer.length);
        readRequest.setStateID(stateID);
        operations.add(readRequest);
        
        compoundRequest.setOperations(operations);
        List<OperationResponse> operationResponses = makeRequest(compoundRequest);
        getResponse(operationResponses.remove(0), PUTFHResponse.class);
        
        READResponse readResponse = getResponse(operationResponses.remove(0), READResponse.class);
        if(readResponse.isEOF()) {
          return -1;
        }
        bufferOffset = 0;
        bufferLength = readResponse.getLength();
        byte[] data = readResponse.getData();
        assertNotNull(data);
        System.arraycopy(data, readResponse.getStart(), buffer, bufferOffset, bufferLength);
        return read();
      }
      
      @Override
      public void close() throws IOException {

        CompoundRequest compoundRequest = newRequest();
        List<OperationRequest> operations = Lists.newArrayList();
        PUTFHRequest putFhRequest = new PUTFHRequest();
        putFhRequest.setFileHandle(fileHandle);    
        operations.add(putFhRequest);

        CLOSERequest closeRequest = new CLOSERequest();
        closeRequest.setSeqID(stateID.getSeqID() + 1);
        closeRequest.setStateID(stateID);
        operations.add(closeRequest);
        
        compoundRequest.setOperations(operations);
        List<OperationResponse> operationResponses = makeRequest(compoundRequest);
        getResponse(operationResponses.remove(0), PUTFHResponse.class);

        CLOSEResponse closeResponse = getResponse(operationResponses.remove(0), CLOSEResponse.class);

        
        mFileHandleStateID.put(fileHandle, closeResponse.getStateID());
      }
      
    };
  }

  protected ImmutableMap<Integer, Attribute> getAttrs(Path path) {
    FileHandle fileHandle = lookup(path);
    
    if(mFileHandleAttributeMap.containsKey(fileHandle)) {
      return mFileHandleAttributeMap.get(fileHandle);
    }
    
    CompoundRequest compoundRequest = newRequest();
    List<OperationRequest> operations = Lists.newArrayList();
    PUTFHRequest putFhRequest = new PUTFHRequest();
    putFhRequest.setFileHandle(fileHandle);    
    operations.add(putFhRequest);
    operations.add(newGETATTRRequest());
    
    compoundRequest.setOperations(operations);
    List<OperationResponse> operationResponses = makeRequest(compoundRequest);
    
    getResponse(operationResponses.remove(0), PUTFHResponse.class);
    mPathFileHandleMap.put(path, fileHandle);
    mFileHandlePathMap.put(fileHandle, path);
    GETATTRResponse getAttrResponse = getResponse(operationResponses.remove(0), GETATTRResponse.class);
    mFileHandleAttributeMap.put(fileHandle, getAttrResponse.getAttrValues());
    return getAttrs(path);
  }
  public FileStatus getFileStatus(Path path) {
    return new FileStatus(path, getAttrs(path));
  }
}
