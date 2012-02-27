package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
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
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.READDIRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.READRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.REMOVERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RENAMERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RESTOREFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SAVEFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETATTRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.WRITERequest;
import com.google.common.collect.ImmutableList;

public class TestNullFileHandle {

  static class Holder {
    OperationRequestHandler<?, ?> handler;
    OperationRequest request;
    public Holder(OperationRequestHandler<?, ?> handler, OperationRequest request) {
      this.handler = handler;
      this.request = request;
    }
  }

  ImmutableList<Holder> handlers = ImmutableList.<Holder>builder()
      .add(new Holder(new ACCESSHandler(), new ACCESSRequest()))
      .add(new Holder(new CLOSEHandler(), new CLOSERequest()))
      .add(new Holder(new COMMITHandler(), new COMMITRequest()))
      .add(new Holder(new CREATEHandler(), new CREATERequest()))
      .add(new Holder(new GETATTRHandler(), new GETATTRRequest()))
      .add(new Holder(new GETFHHandler(), new GETFHRequest()))
      .add(new Holder(new LOOKUPHandler(), new LOOKUPRequest()))
      .add(new Holder(new OPENCONFIRMHandler(), new OPENCONFIRMRequest()))
      .add(new Holder(new OPENHandler(), new OPENRequest()))
      .add(new Holder(new READDIRHandler(), new READDIRRequest()))
      .add(new Holder(new READHandler(), new READRequest()))
      .add(new Holder(new REMOVEHandler(), new REMOVERequest()))
      .add(new Holder(new RENAMEHandler(), new RENAMERequest()))
      .add(new Holder(new RESTOREFHHandler(), new RESTOREFHRequest()))
      .add(new Holder(new SAVEFHHandler(), new SAVEFHRequest()))
      .add(new Holder(new SETATTRHandler(), new SETATTRRequest()))
      .add(new Holder(new WRITEHandler(), new WRITERequest()))
      .build();


  NFS4Handler server;
  Session session;

  @Before
  public void setup() throws NFS4Exception {
    server = mock(NFS4Handler.class);
    session = mock(Session.class);
  }

  @Test
  public void testNullFileHandle() throws Exception {
    for(Holder holder : handlers) {
      // use reflection to get around generic issues
      Method method = holder.handler.getClass().getMethod("handle", NFS4Handler.class, Session.class, OperationRequest.class);
      Status response = (Status)method.invoke(holder.handler, server, session, holder.request);
      assertEquals(holder.handler.getClass().getName(), NFS4ERR_NOFILEHANDLE, response.getStatus());

    }
  }
}
