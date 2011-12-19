package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Callback;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class SETCLIENTIDRequest extends OperationRequest {

  protected ClientID mClientID;
  protected Callback mCallback;
  protected int mCallbackIdent;
  
  @Override
  public void read(RPCBuffer buffer) {
    mClientID = new ClientID();
    mClientID.read(buffer);
    mCallback = new Callback();
    mCallback.read(buffer);
    mCallbackIdent = buffer.readUint32();
  }

  @Override
  public void write(RPCBuffer buffer) {
    mClientID.write(buffer);
    mCallback.write(buffer);
    buffer.writeUint32(mCallbackIdent);
  }

  @Override
  public int getID() {
    return NFS4_OP_SETCLIENTID;
  }

  public ClientID getClientID() {
    return mClientID;
  }

  public void setClientID(ClientID clientID) {
    this.mClientID = clientID;
  }

  public Callback getCallback() {
    return mCallback;
  }

  public void setCallback(Callback callback) {
    this.mCallback = callback;
  }

  public int getCallbackIdent() {
    return mCallbackIdent;
  }

  public void setCallbackIdent(int callbackIdent) {
    this.mCallbackIdent = callbackIdent;
  }
  
}
