package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.TestUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCResponse;
import com.cloudera.hadoop.hdfs.nfs.security.Credentials;

public class TestRPC {

  @Test
  public void testRequestWire() throws Exception {
    RPCRequest base = RPCTestUtil.createRequest();
    RPCRequest copy = new RPCRequest();
    copy(base, copy);
    assertEquals(base.getMessageType(), copy.getMessageType());
    assertEquals(base.getRpcVersion(), copy.getRpcVersion());
    assertEquals(base.getProgram(), copy.getProgram());
    assertEquals(base.getProgramVersion(), copy.getProgramVersion());
    assertEquals(base.getProcedure(), copy.getProcedure());
    assertEquals(base.getCredentials().getCredentialsFlavor(), 
        copy.getCredentials().getCredentialsFlavor());
    deepEquals(base, copy);
  }
  
  @Test(expected = UnsupportedOperationException.class)
  public void testUnknownCreds() throws Exception {
    RPCRequest request = RPCTestUtil.createRequest();
    Credentials creds = mock(Credentials.class);
    when(creds.getCredentialsFlavor()).thenReturn(-1);
    request.setCredentials(creds);
    copy(request, new RPCRequest());
  }
  
  @Test
  public void testResponseWire() throws Exception {
    RPCResponse base = RPCTestUtil.createResponse();
    RPCResponse copy = new RPCResponse();
    copy(base, copy);
    assertEquals(base.getMessageType(), copy.getMessageType());
    assertEquals(base.getReplyState(), copy.getReplyState());
    assertEquals(base.getAcceptState(), copy.getAcceptState());
    deepEquals(base, copy);
  }
  
  @Test
  public void testResponseWireAuthError() throws Exception {
    RPCResponse base = RPCTestUtil.createResponse();
    base.setReplyState(RPC_REPLY_STATE_DENIED);
    base.setAuthState(RPC_REJECT_AUTH_ERROR);
    RPCResponse copy = new RPCResponse();
    copy(base, copy);
    assertEquals(base.getMessageType(), copy.getMessageType());
    assertEquals(base.getReplyState(), copy.getReplyState());
    assertEquals(base.getAcceptState(), copy.getAcceptState());
    deepEquals(base, copy);
  }  

}
