package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.PUTFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.PUTFHResponse;

public class PUTFHHandler extends OperationRequestHandler<PUTFHRequest, PUTFHResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(PUTFHHandler.class);
  @Override
  protected PUTFHResponse doHandle(NFS4Handler server, Session session,
      PUTFHRequest request) throws NFS4Exception {
    session.setCurrentFileHandle(request.getFileHandle());
    PUTFHResponse response = createResponse();
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected PUTFHResponse createResponse() {
    return new PUTFHResponse();
  }

}
