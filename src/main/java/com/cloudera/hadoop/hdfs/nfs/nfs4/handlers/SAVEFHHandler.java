package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SAVEFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SAVEFHResponse;

public class SAVEFHHandler extends OperationRequestHandler<SAVEFHRequest, SAVEFHResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(SAVEFHHandler.class);

  @Override
  protected SAVEFHResponse doHandle(NFS4Handler server, Session session,
      SAVEFHRequest request) throws NFS4Exception {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    session.setSavedFileHandle(session.getCurrentFileHandle());
    SAVEFHResponse response = createResponse();
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected SAVEFHResponse createResponse() {
    return new SAVEFHResponse();
  }

}
