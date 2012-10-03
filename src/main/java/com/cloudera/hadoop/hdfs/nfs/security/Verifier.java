package com.cloudera.hadoop.hdfs.nfs.security;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_VERIFIER_GSS;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.RPC_VERIFIER_NULL;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public abstract class Verifier implements MessageBase {


  public abstract int getFlavor();

  public static Verifier readVerifier(int flavor, RPCBuffer buffer) {
    Verifier verifer;
    if(flavor == RPC_VERIFIER_NULL) {
      verifer = new VerifierNone();
    } else if(flavor == RPC_VERIFIER_GSS) {
      verifer = new VerifierGSS();
    } else {
      throw new UnsupportedOperationException("Unsupported verifier flavor" + flavor);
    }
    verifer.read(buffer);
    return verifer;
  }

}
