package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class COMMITResponse extends OperationResponse implements Status {

  protected int mStatus;
  protected OpaqueData8 mVerifer;
  
  @Override
  public void read(RPCBuffer buffer) {
    mStatus = buffer.readUint32();
    mVerifer = new OpaqueData8();
    mVerifer.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    mVerifer.write(buffer);
  }
  @Override
  public int getStatus() {
    return mStatus;
  }
  @Override
  public void setStatus(int status) {
    this.mStatus = status;
  }
  @Override
  public int getID() {
    return NFS4_OP_COMMIT;
  }
  public OpaqueData8 getVerfier() {
    return mVerifer;
  }
  public void setVerifer(OpaqueData8 verifer) {
    mVerifer = verifer;
  }
}
