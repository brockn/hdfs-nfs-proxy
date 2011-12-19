package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.security.Credentials;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsNull;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsUnix;

public class RPCRequest extends RPCPacket {
  
  protected static final Logger LOGGER = LoggerFactory.getLogger(RPCRequest.class);
  
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
  public void write(RPCBuffer buffer ) {
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
    if(mCredentialsFlavor == RPC_AUTH_NULL) {
      mCredentials = new CredentialsNull(); 
    } else if(mCredentialsFlavor == RPC_AUTH_UNIX) {
      mCredentials = new CredentialsUnix(); 
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
    if(mCredentials != null) {
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