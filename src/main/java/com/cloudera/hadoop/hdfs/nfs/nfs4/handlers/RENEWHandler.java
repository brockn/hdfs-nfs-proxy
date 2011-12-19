package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Client;
import com.cloudera.hadoop.hdfs.nfs.nfs4.ClientFactory;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RENEWRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.RENEWResponse;

public class RENEWHandler extends OperationRequestHandler<RENEWRequest, RENEWResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RENEWHandler.class);

  @Override
  protected RENEWResponse doHandle(NFS4Handler server, Session session,
      RENEWRequest request) throws NFS4Exception {
    ClientFactory clientFactory = server.getClientFactory();
    Client client = clientFactory.getByShortHand(request.getClientID());
    if(client == null) {
      throw new NFS4Exception(NFS4ERR_STALE_CLIENTID);
    }
    // not doing much right now because we don't use leases
    client.setRenew(System.currentTimeMillis());
    RENEWResponse response = createResponse();
    OpaqueData8 verifer = new OpaqueData8();
    verifer.setData(Bytes.toBytes(server.getStartTime()));
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected RENEWResponse createResponse() {
    return new RENEWResponse();
  }

}
