package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.XID;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsNull;


public class RPCTestUtil {
  public static RPCRequest createRequest() {
    int xid = XID.next();
    RPCRequest base = new RPCRequest(xid, RPC_VERSION);
    base.setProgram(NFS_PROG);
    base.setProcedure(NFS_PROC_COMPOUND);
    base.setProgramVersion(NFS_VERSION);
    assertEquals(xid, base.getXid());
    assertEquals(RPC_MESSAGE_TYPE_CALL, base.getMessageType());
    assertEquals(RPC_VERSION, base.getRpcVersion());
    assertEquals(NFS_PROG, base.getProgram());
    assertEquals(NFS_VERSION, base.getProgramVersion());
    assertEquals(NFS_PROC_COMPOUND, base.getProcedure());
    base.setCredentials(new CredentialsNull());
    assertEquals(RPC_AUTH_NULL, base.getCredentials().getCredentialsFlavor());
    return base;
  }

  public static RPCResponse createResponse() {
    int xid = XID.next();
    RPCResponse response = new RPCResponse(xid, RPC_VERSION);
    response.setProgram(NFS_PROG);
    response.setProcedure(NFS_PROC_COMPOUND);
    response.setProgramVersion(NFS_VERSION);
    response.setReplyState(RPC_REPLY_STATE_ACCEPT);
    response.setAcceptState(RPC_ACCEPT_STATE_ACCEPT);
    response.setAuthState(RPC_REJECT_AUTH_ERROR);
    
    assertEquals(xid, response.getXid());
    assertEquals(RPC_MESSAGE_TYPE_REPLY, response.getMessageType());
    assertEquals(RPC_VERSION, response.getRpcVersion());
    assertEquals(NFS_PROG, response.getProgram());
    assertEquals(NFS_VERSION, response.getProgramVersion());
    assertEquals(NFS_PROC_COMPOUND, response.getProcedure());
    assertEquals(RPC_REPLY_STATE_ACCEPT, response.getReplyState());
    assertEquals(RPC_ACCEPT_STATE_ACCEPT, response.getAcceptState());
    assertEquals(RPC_REJECT_AUTH_ERROR, response.getAuthState());
    
    return response;
  }
}
