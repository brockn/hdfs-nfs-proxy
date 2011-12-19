package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class SETCLIENTIDCONFIRMRequest extends OperationRequest {
  protected long mClientID;
  protected OpaqueData8 mVerifer;
  @Override
  public void read(RPCBuffer buffer) {
    mClientID = buffer.readUint64();
    mVerifer = new OpaqueData8();
    mVerifer.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mClientID);
    mVerifer.write(buffer);
  }

  @Override
  public int getID() {
    return NFS4_OP_SETCLIENTID_CONFIRM;
  }

  public long getClientID() {
    return mClientID;
  }

  public void setClientID(long clientID) {
    this.mClientID = clientID;
  }

  public OpaqueData8 getVerifer() {
    return mVerifer;
  }

  public void setVerifer(OpaqueData8 verifer) {
    this.mVerifer = verifer;
  }
  
}
