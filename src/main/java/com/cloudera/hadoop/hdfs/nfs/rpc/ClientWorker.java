package com.cloudera.hadoop.hdfs.nfs.rpc;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.LogUtils;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RequiresCredentials;
import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;

/**
 * ClientWorker handles a socket connection to the server and terminates when the
 * input stream throws an EOF Exception. As all requests execute in an async
 * fashion, the worker sleeps under two conditions: 1) the client retransmits
 * requests past a certain threshold 2) the client submits a requests and
 * the number of in progress requests exceeds some threshold. This sleep
 * obviously stops the client from sending addition requests and likely
 * causes the underlying TCP client to be sent the TCP SLOW DOWN packet.
 * 
 * @param <REQUEST>
 * @param <RESPONSE>
 */
class ClientWorker<REQUEST extends MessageBase, RESPONSE extends MessageBase> extends Thread {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ClientWorker.class);
  protected static final long RETRANSMIT_PENALTY_THRESHOLD = 3L;
  protected static final long RETRANSMIT_PENALTY_TIME = 1000L;
  protected static final long TOO_MANY_PENDING_REQUESTS_PAUSE = 1000L;
  protected static final long EXECUTOR_THREAD_POOL_FULL_PAUSE = 1000L;

  protected int mRestransmitPenaltyThreshold;
  protected int mMaxPendingRequests;
  protected Socket mClient;
  protected String mClientName;
  protected RPCServer<REQUEST, RESPONSE> mRPCServer;
  protected OutputStreamHandler mOutputHandler;
  protected RPCHandler<REQUEST, RESPONSE> mHandler;
  /**
   * Number of retransmits received
   */
  protected long mRetransmists = 0L;
  protected String mSessionID;
  protected Configuration mConfiguration;   
  protected static final AtomicInteger SESSIONID = new AtomicInteger(Integer.MAX_VALUE);
  
  public ClientWorker(Configuration conf, RPCServer<REQUEST, RESPONSE> server, RPCHandler<REQUEST, RESPONSE> handler, Socket client) {
    mConfiguration = conf;
    mRPCServer = server;
    mHandler = handler;
    mClient = client;
    mClientName = mClient.getInetAddress().getCanonicalHostName() + ":" + mClient.getPort();
    mSessionID = "0x" + Integer.toHexString(SESSIONID.addAndGet(-5));
    setName("RPCServer-" + mClientName);
    
    mMaxPendingRequests = mConfiguration.getInt(RPC_MAX_PENDING_REQUESTS, 20);
    mRestransmitPenaltyThreshold = mConfiguration.getInt(RPC_RETRANSMIT_PENALTY_THRESHOLD, 3);
  }
  
  public void run() {
    InputStream in = null;
    OutputStream out = null;
    RPCRequest request = null;
    try {
      mClient.setTcpNoDelay(true);
      mClient.setPerformancePreferences(0, 1, 0);
      in = mClient.getInputStream();
      out = mClient.getOutputStream();
      mOutputHandler = new OutputStreamHandler(out, mClientName);
      mOutputHandler.setDaemon(true);
      mOutputHandler.start();
      while(true) {
        // request is used to indicate if we should send
        // a failure packet in case of an error
        request = null;
        if(mRetransmists >= mRestransmitPenaltyThreshold) {
          mRetransmists = 0L;
          mHandler.incrementMetric("RETRANSMIT_PENALTY_BOX", 1);
          Thread.sleep(RETRANSMIT_PENALTY_TIME);
          LOGGER.warn(mSessionID + " Client " + mClientName + " is going in the penalty box (SLOWDOWN)");
        }        
        mRetransmists = mRetransmists > 0 ? mRetransmists : 0;
        
        while(getRequestsInProgress().size() > mMaxPendingRequests) {
          mHandler.incrementMetric("TOO_MANY_PENDING_REQUESTS", 1);
          LOGGER.warn(mSessionID + " Client " + mClientName + " is waiting for pending requests to finish (SLOWDOWN)");
          Thread.sleep(TOO_MANY_PENDING_REQUESTS_PAUSE);
        }
        
        RPCBuffer requestBuffer = RPCBuffer.from(in);
        mHandler.incrementMetric("CLIENT_BYTES_READ", requestBuffer.length());
        request = new RPCRequest();
        request.read(requestBuffer);
        if(request.getRpcVersion() != RPC_VERSION) {
          LOGGER.info(mSessionID + " Denying client due to bad RPC version " + request.getRpcVersion() + " for " + mClientName);
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_REJECT_MISMATCH);
          writeRPCResponse(response);
        } else if(request.getCredentials() == null){
          LOGGER.info(mSessionID + " Denying client due to null credentials for " + mClientName);
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_REJECT_AUTH_ERROR);
          response.setAuthState(RPC_AUTH_STATUS_TOOWEAK);
          writeRPCResponse(response);
        } else if(request.getProcedure() == NFS_PROC_NULL){
          LOGGER.info(mSessionID + " Handling NFS NULL Procedure for " + mClientName);
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_ACCEPT);
          response.setAcceptState(RPC_ACCEPT_SUCCESS);
          writeRPCResponse(response);
        } else if(!(request.getCredentials() instanceof AuthenticatedCredentials)) {
          LOGGER.info(mSessionID + " Denying client due to non-authenticated credentials for " + mClientName);
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_REJECT_AUTH_ERROR);
          response.setAuthState(RPC_AUTH_STATUS_TOOWEAK);
          writeRPCResponse(response);
        } else if(request.getProcedure() == NFS_PROC_COMPOUND){  
          if(LOGGER.isDebugEnabled()) {
            LOGGER.debug(mSessionID + " Handling NFS Compound for " + mClientName);
          }
          Set<Integer> requestsInProgress = getRequestsInProgress();
          synchronized (requestsInProgress) {
            if(requestsInProgress.contains(request.getXid())) {
              mRetransmists++;
              mHandler.incrementMetric("RESTRANMITS", 1);
            } else {
              mRetransmists--;
              requestsInProgress.add(request.getXid());
              ClientTask<REQUEST, RESPONSE> task = new ClientTask<REQUEST, RESPONSE>(mClientName, mSessionID, this, mHandler,  request, requestBuffer);
              this.schedule(task);
            }
          }
        } else {
          throw new UnsupportedOperationException("Unknown Proc " + request.getProcedure());
        }
      }
    } catch(EOFException e) {
      LOGGER.info(mSessionID + " Shutdown worker for client " + mClientName);
    } catch (Exception e) {
      LOGGER.error(mSessionID + " Error from client " + mClientName, e);
      if(request != null) {
        try {
          RPCResponse response = new RPCResponse(request.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_ACCEPT_SYSTEM_ERR);
          writeRPCResponse(response);
        } catch (Exception x) {
          LOGGER.error(mSessionID + " Error writing failure packet", x);
        }
      }
    } finally {
      if(mOutputHandler != null) {
        mOutputHandler.close();
      }
      IOUtils.closeSocket(mClient);
      Map<Socket, ClientWorker<REQUEST, RESPONSE>> clients = mRPCServer.getClients();
      clients.remove(mClient);
    }
  }
  protected void writeApplicationResponse(int xid, MessageBase applicationResponse) throws IOException {
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug(mSessionID + " Writing " + applicationResponse.getClass().getSimpleName() + " to "  + mClientName);
    }
    RPCBuffer responseBuffer = new RPCBuffer();
    RPCResponse response = new RPCResponse(xid, RPC_VERSION);
    response.setReplyState(RPC_REPLY_STATE_ACCEPT);
    response.setAcceptState(RPC_ACCEPT_SUCCESS);
    response.write(responseBuffer);
    applicationResponse.write(responseBuffer);
    responseBuffer.flip();
    mOutputHandler.add(responseBuffer);
    mHandler.incrementMetric("CLIENT_BYTES_WROTE", responseBuffer.length());
  }
  protected void writeRPCResponse(RPCResponse response) throws IOException {
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug(mSessionID + " Writing bare RPC Response to "  + mClientName);
    }
    RPCBuffer responseBuffer = new RPCBuffer();
    response.write(responseBuffer);
    responseBuffer.flip();
    mOutputHandler.add(responseBuffer);
    mHandler.incrementMetric("CLIENT_BYTES_WROTE", responseBuffer.length());
  }
  
  protected Map<Integer, MessageBase> getResponseCache() {
    return mRPCServer.getResponseCache();
  }
  public Set<Integer> getRequestsInProgress() {
    return mRPCServer.getRequestsInProgress();
  }
  
  /**
   * Schedule task blocking until successful or interrupted.
   * @param task
   * @throws InterruptedException if interrupted while waiting
   * for room in the thread pool.
   */
  public void schedule(ClientTask<REQUEST, RESPONSE> task) throws InterruptedException {
    ExecutorService executorServer = mRPCServer.getExecutorService();
    while(true) {
      try {
        executorServer.execute(task);
        break;
      } catch(RejectedExecutionException ex) {
        mHandler.incrementMetric("THREAD_POOL_FULL", 1);
        LOGGER.warn("Task rejected, thread pool at max capacity: " + task.mClientName + " " + task.mSessionID);
        Thread.sleep(EXECUTOR_THREAD_POOL_FULL_PAUSE);
      }
    }
  }

  /**
   * Task submitted to Executor for execution of the 
   * client's request.
   */
  protected static class ClientTask<REQUEST extends MessageBase, RESPONSE extends MessageBase> implements Runnable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ClientTask.class);
    protected String mClientName;
    protected String mSessionID;
    protected RPCHandler<REQUEST, RESPONSE> mHandler;
    protected ClientWorker<REQUEST, RESPONSE> mClientWorker;
    protected RPCRequest mRequest;
    protected RPCBuffer mBuffer;
    
    public ClientTask(String clientName, String sessionID, 
        ClientWorker<REQUEST, RESPONSE> clientWorker, RPCHandler<REQUEST, RESPONSE> handler, 
        RPCRequest request, RPCBuffer buffer) {
      mClientName = clientName;
      mSessionID = sessionID;
      mClientWorker = clientWorker;
      mHandler = handler;
      mRequest = request;
      mBuffer = buffer;
    }
    
    @Override
    public void run() {
      Set<Integer> requestsInProgress = mClientWorker.getRequestsInProgress();
      Map<Integer, MessageBase> responseCache = mClientWorker.getResponseCache();
      try {
        /*
         *   TODO ensure credentials are the same for request/cached response.
         */
        MessageBase applicationResponse = responseCache.get(mRequest.getXid());
        if(applicationResponse == null) {
          REQUEST applicationRequest = mHandler.createRequest();
          applicationRequest.read(mBuffer);
          if(applicationRequest instanceof RequiresCredentials) {
            RequiresCredentials requiresCredentials = (RequiresCredentials)applicationRequest;
            // check to ensure it's auth creds is above
            requiresCredentials.setCredentials((AuthenticatedCredentials)mRequest.getCredentials());          
          }
          try { 
            applicationResponse = mHandler.process(mRequest, applicationRequest, mClientName, mSessionID);
          } catch(RuntimeException x) {
            LOGGER.warn("Error reading buffer: " + LogUtils.dump(applicationResponse), x);
            throw x;
          }
          responseCache.put(mRequest.getXid(), applicationResponse);
        }
        mClientWorker.writeApplicationResponse(mRequest.getXid(), applicationResponse);
      } catch(Exception ex) {
        LOGGER.error(mSessionID + " Error from client " + mClientName, ex);
        try {
          RPCResponse response = new RPCResponse(mRequest.getXid(), RPC_VERSION);
          response.setReplyState(RPC_REPLY_STATE_DENIED);
          response.setAcceptState(RPC_ACCEPT_SYSTEM_ERR);
          mClientWorker.writeRPCResponse(response);
        } catch (Exception x) {
          LOGGER.error(mSessionID + " Error writing failure packet", x);
        }
      } finally {
        requestsInProgress.remove(mRequest.getXid());
      }
    }
  }
}