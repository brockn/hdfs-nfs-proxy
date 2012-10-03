package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_NOTSUPP;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_SERVERFAULT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4_OK;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.handlers.OperationRequestHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OperationRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.CompoundResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OperationResponse;
import com.cloudera.hadoop.hdfs.nfs.nfs4.state.HDFSState;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;

public class NFS4AsyncFuture extends AbstractFuture<CompoundResponse> 
implements AsyncFuture<CompoundResponse> {
  protected static final Logger LOGGER = Logger.getLogger(NFS4AsyncFuture.class);
  private final HDFSState hdfsState;
  private final Session session;
  private final UserGroupInformation ugi;
  private final List<OperationRequest> requests;
  private final List<OperationResponse> responses;
  
  private int lastStatus;
  public NFS4AsyncFuture(HDFSState hdfsState, Session session, UserGroupInformation ugi) {
    this.hdfsState = hdfsState;
    this.session = session;
    this.ugi = ugi;
    this.requests = Lists.newArrayList(session.getCompoundRequest().getOperations());
    this.responses = Lists.newArrayList();
    this.lastStatus = NFS4_OK;
  }
  @Override
  public AsyncFuture.Complete makeProgress() {
    try {
      AsyncFuture.Complete result = ugi.doAs(new PrivilegedExceptionAction<AsyncFuture.Complete>() {
        @Override
        public AsyncFuture.Complete run() throws Exception {
          return doMakeProgress();
        }
      });   
      if(result != AsyncFuture.Complete.COMPLETE) {
        return AsyncFuture.Complete.RETRY;
      }
      CompoundResponse response = new CompoundResponse();
      response.setStatus(lastStatus);
      response.setOperations(responses);
      set(response);
      hdfsState.incrementMetric("NFS_COMMANDS", 1);
    } catch (Exception ex) {
      if (ex instanceof UndeclaredThrowableException && ex.getCause() != null) {
        Throwable throwable = ex.getCause();
        setException(throwable);
        if (throwable instanceof Exception) {
          ex = (Exception) throwable;
        } else if (throwable instanceof Error) {
          // something really bad happened
          LOGGER.error(session.getSessionID() + " Unhandled Error", throwable);
        } else {
          LOGGER.error(session.getSessionID() + " Unhandled Throwable", throwable);
        }
      } else {
        setException(ex);
      }
      LOGGER.warn(session.getSessionID() + " Unhandled Exception", ex);
      CompoundResponse response = new CompoundResponse();
      if (ex instanceof NFS4Exception) {
        response.setStatus(((NFS4Exception) ex).getError());
      } else if (ex instanceof UnsupportedOperationException) {
        response.setStatus(NFS4ERR_NOTSUPP);
      } else {
        LOGGER.warn(session.getSessionID() + " Setting SERVERFAULT for " + session.getClientAddress()
            + " for " + session.getCompoundRequest().getOperations());
        response.setStatus(NFS4ERR_SERVERFAULT);
      }
      set(response);
    }
    return AsyncFuture.Complete.COMPLETE;
  }
  
  public AsyncFuture.Complete doMakeProgress() throws IOException {
    if(lastStatus != NFS4_OK) {
      return AsyncFuture.Complete.COMPLETE;
    }
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    for (Iterator<OperationRequest> iterator = requests.iterator(); iterator.hasNext();) {
      OperationRequest request = iterator.next();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(session.getSessionID() + " " + session.getXIDAsHexString() + 
            " processing " + request + " for " + username);
      }
      OperationRequestHandler<OperationRequest, OperationResponse> requestHandler = OperationFactory.getHandler(request.getID());
      if(requestHandler.wouldBlock(hdfsState, session, request)) {
        return AsyncFuture.Complete.RETRY;
      }
      iterator.remove();
      OperationResponse response = requestHandler.handle(hdfsState, session,  request);
      responses.add(response);
      lastStatus = response.getStatus();
      if (lastStatus != NFS4_OK) {
        LOGGER.warn(session.getSessionID() + " Quitting due to " + lastStatus + " on "
            + request.getClass().getSimpleName() + " for " + username);
        return AsyncFuture.Complete.COMPLETE;
      }
      hdfsState.incrementMetric("NFS_" + request.getClass().getSimpleName(), 1);
      hdfsState.incrementMetric("NFS_OPERATIONS", 1);
    }
    return AsyncFuture.Complete.COMPLETE;
  }
  @Override
  public String toString() {
    return "NFS4AsyncFuture [session=" + session + ", ugi=" + ugi
        + ", requests=" + requests + ", responses=" + responses
        + ", lastStatus=" + lastStatus + "]";
  }
}
