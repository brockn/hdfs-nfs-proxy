package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class VerifierGSS extends Verifier {
  protected OpaqueData mOpaqueData;

  @Override
  public void read(RPCBuffer buffer) {
    int length = buffer.readUint32();
    mOpaqueData = new OpaqueData(length);
    mOpaqueData.read(buffer);

  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mOpaqueData.getSize());
    mOpaqueData.write(buffer);
  }

  public void set(byte[] data) {
    mOpaqueData = new OpaqueData(data.length);
    mOpaqueData.setData(data);
  }

  public byte[] get() {
    return mOpaqueData.getData();
  }

  @Override
  public int getFlavor() {
    return RPC_VERIFIER_GSS;
  }

}
