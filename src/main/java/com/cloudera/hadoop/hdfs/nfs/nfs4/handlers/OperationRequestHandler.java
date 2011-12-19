package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OperationRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OperationResponse;

public abstract class OperationRequestHandler<IN extends OperationRequest, OUT extends OperationResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(OperationRequestHandler.class);

  public OUT handle(NFS4Handler server, Session session, IN request) throws IOException {
    try {
      return doHandle(server, session, request);
    } catch(Exception ex) {
      server.incrementMetric("EXCEPTION_" + ex.getClass().getSimpleName(), 1);
      OUT response = createResponse();
      boolean log = true;
      if(ex instanceof NFS4Exception) {
        NFS4Exception nfsEx = (NFS4Exception)ex;
        log = !nfsEx.shouldLog();
        response.setStatus(nfsEx.getError());
      } else if(ex instanceof FileNotFoundException) {
        response.setStatus(NFS4ERR_NOENT);
      } else if(ex instanceof IOException) {
        response.setStatus(NFS4ERR_IO);
      } else if(ex instanceof IllegalArgumentException) {
        response.setStatus(NFS4ERR_INVAL);
      } else if(ex instanceof UnsupportedOperationException) {
        response.setStatus(NFS4ERR_NOTSUPP);
      } else if(ex instanceof AccessControlException) {
        response.setStatus(NFS4ERR_PERM);
      } else {
        response.setStatus(NFS4ERR_SERVERFAULT);
      }
      String msg = session.getSessionID() + " Error for client " + 
          session.getClientHostPort() + " and " + response.getClass().getSimpleName();
      if(log || LOGGER.isDebugEnabled()) {
        LOGGER.warn(msg, ex);
      } else {
        LOGGER.warn(msg);
      }
      return response;
    }
  }
  protected abstract OUT doHandle(NFS4Handler server, Session session, IN request) 
      throws NFS4Exception, IOException, UnsupportedOperationException;  
  protected abstract OUT createResponse();
}
