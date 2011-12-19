package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.CLOSERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CLOSEResponse;

public class CLOSEHandler extends OperationRequestHandler<CLOSERequest, CLOSEResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CLOSEHandler.class);
  @Override
  protected CLOSEResponse doHandle(NFS4Handler server, Session session,
      CLOSERequest request) throws NFS4Exception, IOException {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    StateID stateID = server.close(session.getSessionID(), request.getStateID(), 
        request.getSeqID(), session.getCurrentFileHandle());
    CLOSEResponse response = createResponse();
    response.setStateID(stateID);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected CLOSEResponse createResponse() {
    return new CLOSEResponse();
  }

}
