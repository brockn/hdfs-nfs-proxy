package com.cloudera.hadoop.hdfs.nfs.nfs4;


import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.hadoop.hdfs.nfs.TestUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CompoundRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCRequest;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCResponse;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCTestUtil;
import com.cloudera.hadoop.hdfs.nfs.security.Credentials;

public class NetworkClient extends BaseClient {

  protected NFS4Server mNFS4Server;
  protected Socket mClient;
  protected InputStream mInputStream;
  protected OutputStream mOutputStream;
  protected int mPort;
  public NetworkClient() throws IOException {    
    Configuration conf = new Configuration();
    mNFS4Server = new NFS4Server();
    mNFS4Server.setConf(conf);
    mNFS4Server.start(0);
    mPort = mNFS4Server.getPort();
    
    mClient = new Socket("localhost", mPort);
    
    mInputStream = mClient.getInputStream();
    mOutputStream = mClient.getOutputStream();
    
    initialize();

  }
  
  @Override
  protected CompoundResponse doMakeRequest(CompoundRequest request) throws IOException {
    
    RPCBuffer buffer = new RPCBuffer();
    RPCRequest rpcRequest = RPCTestUtil.createRequest();
    rpcRequest.setCredentials((Credentials)TestUtils.newCredentials());
    rpcRequest.write(buffer);
    request.write(buffer);
    
    buffer.write(mOutputStream);
    
    buffer = RPCBuffer.from(mInputStream);
    RPCResponse rpcResponse = new RPCResponse();
    rpcResponse.read(buffer);
    
    assertEquals(rpcRequest.getXid(), rpcResponse.getXid());
    assertEquals(RPC_REPLY_STATE_ACCEPT, rpcResponse.getReplyState());
    assertEquals(RPC_ACCEPT_SUCCESS, rpcResponse.getAcceptState());
    
    CompoundResponse response = new CompoundResponse();
    response.read(buffer);
    return response;
  }
  
  @Override
  public void shutdown() {
    mNFS4Server.shutdown();
  }

}
