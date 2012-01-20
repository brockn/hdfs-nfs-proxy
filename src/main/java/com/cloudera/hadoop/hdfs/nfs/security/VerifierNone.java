package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class VerifierNone extends Verifier {
  @Override
  public void read(RPCBuffer buffer) {
    int length = buffer.readUint32();
    if(length != 0) {
      throw new RuntimeException("Length must be zero " + length);
    }
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(0);
  }

  @Override
  public int getFlavor() {
    return RPC_VERIFIER_NULL;
  }

}
