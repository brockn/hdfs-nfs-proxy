package com.cloudera.hadoop.hdfs.nfs.rpc;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RPCResponse extends RPCPacket {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RPCResponse.class);

  protected int mReplyState, mAcceptState, mAuthState;
  protected int mVerifierFlavor, mVeriferLength;
  
  public RPCResponse() {
    
  }
  public RPCResponse(int xid, int rpcVersion) {
    this.mXid = xid;
    this.mRpcVersion = rpcVersion;
    
    this.mMessageType = RPC_MESSAGE_TYPE_REPLY;
    this.mReplyState = RPC_REPLY_STATE_ACCEPT;
    this.mVerifierFlavor = RPC_VERIFIER_NULL;
    this.mVeriferLength = 0;
    this.mAcceptState = RPC_ACCEPT_STATE_ACCEPT;
  }
  
  @Override
  public void write(RPCBuffer buffer) {
    super.write(buffer);
    
    buffer.writeInt(mReplyState);
    /*
     * It looks like if reply state is not
     * accept, the next value acceptState
     */
    if(mReplyState == RPC_REPLY_STATE_ACCEPT) {
      buffer.writeInt(mVerifierFlavor);
      buffer.writeInt(mVeriferLength);
      buffer.writeInt(mAcceptState);
    } else if(mReplyState == RPC_REPLY_STATE_DENIED) {
      buffer.writeInt(mAcceptState);
      if(mAcceptState == RPC_REJECT_AUTH_ERROR) {
        buffer.writeInt(mAuthState);
      }
      
    }
  }

  @Override
  public void read(RPCBuffer buffer) {
    super.read(buffer);
    this.mReplyState = buffer.readInt();
    /*
     * It looks like if reply state is not
     * accept, the next value acceptState
     */
    if(mReplyState == RPC_REPLY_STATE_ACCEPT) {
      this.mVerifierFlavor = buffer.readInt();
      this.mVeriferLength = buffer.readInt();
      buffer.skip(mVeriferLength);
      this.mAcceptState = buffer.readInt();    
    } else if(mReplyState == RPC_REPLY_STATE_DENIED) {
      this.mAcceptState = buffer.readInt();    
      if(mAcceptState == RPC_REJECT_AUTH_ERROR) {
        mAuthState = buffer.readInt();
      }
    }
    /*
     * Probaly should throw exception if 
     * accept state is not accepted?
     */
  }
  
  public int getAuthState() {
    return mAuthState;
  }
  public void setAuthState(int authState) {
    this.mAuthState = authState;
  }
  @Override
  public String toString() {
    return "RPCResponse [replyState=" + mReplyState + ", acceptState="
        + mAcceptState + "]";
  }
  public int getVerifierFlavor() {
    return mVerifierFlavor;
  }
  public void setVerifierFlavor(int verifierFlavor) {
    this.mVerifierFlavor = verifierFlavor;
  }
  public int getAcceptState() {
    return mAcceptState;
  }
  public void setAcceptState(int acceptState) {
    this.mAcceptState = acceptState;
  }
  public int getReplyState() {
    return mReplyState;
  }
  public void setReplyState(int replyState) {
    this.mReplyState = replyState;
  }

}
