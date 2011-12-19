package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RESTOREFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.RESTOREFHResponse;

public class RESTOREFHHandler extends OperationRequestHandler<RESTOREFHRequest, RESTOREFHResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RESTOREFHHandler.class);

  @Override
  protected RESTOREFHResponse doHandle(NFS4Handler server, Session session,
      RESTOREFHRequest request) throws NFS4Exception {
    if(session.getSavedFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    session.setCurrentFileHandle(session.getSavedFileHandle());
    RESTOREFHResponse response = createResponse();
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected RESTOREFHResponse createResponse() {
    return new RESTOREFHResponse();
  }

}
