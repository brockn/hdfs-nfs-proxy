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

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_ACCESS;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_CLOSE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_COMMIT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_CREATE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_GETATTR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_GETFH;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_LOOKUP;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_OPEN;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_OPEN_CONFIRM;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_PUTFH;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_PUTROOTFH;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_READ;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_READDIR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_REMOVE;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_RENAME;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_RENEW;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_RESTOREFH;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_SAVEFH;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_SETATTR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_SETCLIENTID;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_SETCLIENTID_CONFIRM;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OP_WRITE;

import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.ACCESSHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.CLOSEHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.COMMITHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.CREATEHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.GETATTRHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.GETFHHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.LOOKUPHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.OPENCONFIRMHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.OPENHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.OperationRequestHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.PUTFHHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.PUTROOTFHHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.READDIRHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.READHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.REMOVEHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.RENAMEHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.RENEWHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.RESTOREFHHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.SAVEFHHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.SETATTRHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.SETCLIENTIDCONFIRMHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.SETCLIENTIDHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.WRITEHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.ACCESSRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CLOSERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.COMMITRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CREATERequest;
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
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.REMOVERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RENAMERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RENEWRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RESTOREFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SAVEFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETATTRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETCLIENTIDCONFIRMRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETCLIENTIDRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.WRITERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.ACCESSResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CLOSEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.COMMITResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CREATEResponse;
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
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.REMOVEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.RENAMEResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.RENEWResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.RESTOREFHResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SAVEFHResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETATTRResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETCLIENTIDCONFIRMResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETCLIENTIDResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.WRITEResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableMap;

public class OperationFactory {

  static class Holder {
    Class<? extends OperationRequest> requestClazz;
    Class<? extends OperationResponse> responseClazz;
    OperationRequestHandler<? extends OperationRequest, ? extends OperationResponse> handler;
    public Holder(
        Class<? extends OperationRequest> requestClazz,
        Class<? extends OperationResponse> responseClazz,
        OperationRequestHandler<? extends OperationRequest, ? extends OperationResponse> handler) {
      this.requestClazz = requestClazz;
      this.responseClazz = responseClazz;
      this.handler = handler;
    }
  }
  static ImmutableMap<Integer, Holder> operations = ImmutableMap.<Integer, Holder>builder()
      .put(NFS4_OP_ACCESS, new Holder(ACCESSRequest.class, ACCESSResponse.class, new ACCESSHandler()))
      .put(NFS4_OP_CLOSE, new Holder(CLOSERequest.class, CLOSEResponse.class, new CLOSEHandler()))
      .put(NFS4_OP_CREATE, new Holder(CREATERequest.class, CREATEResponse.class, new CREATEHandler()))
      .put(NFS4_OP_COMMIT, new Holder(COMMITRequest.class, COMMITResponse.class, new COMMITHandler()))
      .put(NFS4_OP_GETATTR, new Holder(GETATTRRequest.class, GETATTRResponse.class, new GETATTRHandler()))
      .put(NFS4_OP_GETFH, new Holder(GETFHRequest.class, GETFHResponse.class, new GETFHHandler()))
      .put(NFS4_OP_LOOKUP, new Holder(LOOKUPRequest.class, LOOKUPResponse.class, new LOOKUPHandler()))
      .put(NFS4_OP_OPEN, new Holder(OPENRequest.class, OPENResponse.class, new OPENHandler()))
      .put(NFS4_OP_OPEN_CONFIRM, new Holder(OPENCONFIRMRequest.class, OPENCONFIRMResponse.class, new OPENCONFIRMHandler()))
      .put(NFS4_OP_READ, new Holder(READRequest.class, READResponse.class, new READHandler()))
      .put(NFS4_OP_REMOVE, new Holder(REMOVERequest.class, REMOVEResponse.class, new REMOVEHandler()))
      .put(NFS4_OP_RENAME, new Holder(RENAMERequest.class, RENAMEResponse.class, new RENAMEHandler()))
      .put(NFS4_OP_RENEW, new Holder(RENEWRequest.class, RENEWResponse.class, new RENEWHandler()))
      .put(NFS4_OP_READDIR, new Holder(READDIRRequest.class, READDIRResponse.class, new READDIRHandler()))
      .put(NFS4_OP_RESTOREFH, new Holder(RESTOREFHRequest.class, RESTOREFHResponse.class, new RESTOREFHHandler()))
      .put(NFS4_OP_SETATTR, new Holder(SETATTRRequest.class, SETATTRResponse.class, new SETATTRHandler()))
      .put(NFS4_OP_SAVEFH, new Holder(SAVEFHRequest.class, SAVEFHResponse.class, new SAVEFHHandler()))
      .put(NFS4_OP_PUTROOTFH, new Holder(PUTROOTFHRequest.class, PUTROOTFHResponse.class, new PUTROOTFHHandler()))
      .put(NFS4_OP_PUTFH, new Holder(PUTFHRequest.class, PUTFHResponse.class, new PUTFHHandler()))
      .put(NFS4_OP_SETCLIENTID, new Holder(SETCLIENTIDRequest.class, SETCLIENTIDResponse.class, new SETCLIENTIDHandler()))
      .put(NFS4_OP_SETCLIENTID_CONFIRM, new Holder(SETCLIENTIDCONFIRMRequest.class, SETCLIENTIDCONFIRMResponse.class, new SETCLIENTIDCONFIRMHandler()))
      .put(NFS4_OP_WRITE, new Holder(WRITERequest.class, WRITEResponse.class, new WRITEHandler()))
      .build();

  /**
   * @param id
   * @return true if id is a supported operation
   */
  public static boolean isSupported(int id) {
    return operations.containsKey(id);
  }
  protected static void checkSupported(int id) {
    if(!isSupported(id)) {
      throw new UnsupportedOperationException("NFS Operation " + id);
    }
  }
  /**
   * Parse a request identified by id from buffer into a NFS specific operation request.
   * @param buffer to read request from
   * @param id of request
   * @return Operation Request associated with id
   */
  public static OperationRequest parseRequest(int id, RPCBuffer buffer) {
    checkSupported(id);
    try {
      OperationRequest operation = operations.get(id).requestClazz.newInstance();
      operation.read(buffer);
      return operation;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parse a response identified by id from buffer into a NFS specific operation response.
   * @param buffer to read response from
   * @param id of request
   * @return Operation Response associated with id
   */
  public static OperationResponse parseResponse(int id, RPCBuffer buffer) {
    checkSupported(id);
    try {
      OperationResponse operation = operations.get(id).responseClazz.newInstance();
      operation.read(buffer);
      return operation;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the stateless handler for for a specific request type.
   * @param id
   * @return a stateless handler for which accepts this request type
   */
  @SuppressWarnings("unchecked")
  public static <IN extends OperationRequest, OUT extends OperationResponse> OperationRequestHandler<IN, OUT> getHandler(int id) {
    checkSupported(id);
    return (OperationRequestHandler<IN, OUT>) operations.get(id).handler;
  }

}
