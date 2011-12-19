package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.GETFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.GETFHResponse;

public class GETFHHandler extends OperationRequestHandler<GETFHRequest, GETFHResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(GETFHHandler.class);

  @Override
  protected GETFHResponse doHandle(NFS4Handler server, Session session,
      GETFHRequest request) throws NFS4Exception {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    GETFHResponse response = createResponse();
    response.setFileHandle(session.getCurrentFileHandle());
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected GETFHResponse createResponse() {
    return new GETFHResponse();
  }

}
