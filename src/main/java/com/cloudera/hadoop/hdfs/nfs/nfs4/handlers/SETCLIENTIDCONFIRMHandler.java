package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static com.google.common.base.Preconditions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Client;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientFactory;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETCLIENTIDCONFIRMRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETCLIENTIDCONFIRMResponse;
public class SETCLIENTIDCONFIRMHandler extends OperationRequestHandler<SETCLIENTIDCONFIRMRequest, SETCLIENTIDCONFIRMResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(SETCLIENTIDCONFIRMHandler.class);

  @Override
  protected SETCLIENTIDCONFIRMResponse doHandle(NFS4Handler server, Session session,
      SETCLIENTIDCONFIRMRequest request) throws NFS4Exception {
    /*
     * TODO should follow RFC 3530 page ~215
     */
    ClientFactory clientFactory = server.getClientFactory();
    Client client = clientFactory.getByShortHand(request.getClientID());
    if(client == null) {
      throw new NFS4Exception(NFS4ERR_STALE_CLIENTID);
    }
    OpaqueData8 verifer = checkNotNull(request.getVerifer(), "verifer");
    if(!verifer.equals(client.getVerifer())) {
      throw new NFS4Exception(NFS4ERR_CLID_INUSE);
    }
    SETCLIENTIDCONFIRMResponse response = createResponse();
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected SETCLIENTIDCONFIRMResponse createResponse() {
    return new SETCLIENTIDCONFIRMResponse();
  }

}
