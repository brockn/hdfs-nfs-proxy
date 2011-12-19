package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OPENCONFIRMRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OPENCONFIRMResponse;

public class OPENCONFIRMHandler extends OperationRequestHandler<OPENCONFIRMRequest, OPENCONFIRMResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(OPENCONFIRMHandler.class);

  @Override
  protected OPENCONFIRMResponse doHandle(NFS4Handler server, Session session,
      OPENCONFIRMRequest request) throws NFS4Exception {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    StateID stateID = server.confirm(request.getStateID(), request.getSeqID(), session.getCurrentFileHandle());
    OPENCONFIRMResponse response = createResponse();
    response.setStateID(stateID);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected OPENCONFIRMResponse createResponse() {
    return new OPENCONFIRMResponse();
  }

}
