package com.cloudera.hadoop.hdfs.nfs.rpc;


import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;

// TODO fix handler generic situation
@SuppressWarnings("rawtypes")
public class RPCServer extends Thread {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RPCServer.class);
  
  protected RPCHandler mHandler;
  protected ConcurrentHashMap<Socket, ClientWorker> mClients = new ConcurrentHashMap<Socket, ClientWorker>();
  protected int mPort;
  protected ServerSocket mServer;
  protected Configuration mConfiguration;
  protected Map<Integer, MessageBase> mResponseCache = 
    Collections.synchronizedMap(new LRUCache<Integer,MessageBase>(500));
  protected Set<Integer> mRequestsInProgress = Collections.synchronizedSet(new HashSet<Integer>());
  protected ExecutorService mExecutor;
  
  
  public RPCServer(RPCHandler rpcHandler, Configuration conf) throws Exception {
    this(rpcHandler, conf, 0);
  }
  
  public RPCServer(RPCHandler rpcHandler, Configuration conf, int port) throws Exception {
    mExecutor = Executors.newFixedThreadPool(conf.getInt(RPC_MAX_THREADS, 50));
    
    mHandler = rpcHandler;
    mConfiguration = conf;
    mPort = port;
    mServer = new ServerSocket(mPort);
    // if port is 0, we are supposed to find a port
    // mPort should then be set to the port we found
    mPort = mServer.getLocalPort();
    setName("RPCServer-" + mHandler.getClass().getSimpleName() + "-" + mPort);
  }
  
  public void run() {
    try {
      mServer.setReuseAddress(true);
      mServer.setPerformancePreferences(0, 1, 0);
      LOGGER.info(mHandler.getClass() + " created server " + mServer + " on " + mPort);
      
      while (true) {
        Socket client = mServer.accept();
        LOGGER.info(mHandler.getClass() + " got client " + client.getInetAddress().getCanonicalHostName());

        ClientWorker worker = new ClientWorker(mHandler, mExecutor, mClients, mResponseCache, mRequestsInProgress, client);
        mClients.put(client, worker);
        worker.start();
      }
    } catch(Exception ex) {
      LOGGER.error("Error on handler " + mHandler.getClass(), ex);
    } finally {
      // first close clients
      for(Socket client : mClients.keySet()) {
        ClientWorker worker = mClients.get(client);
        if(worker != null && worker.isAlive()) {
          worker.interrupt();
        }
        IOUtils.closeSocket(client);
      }
      // then server
      if(mServer != null) {
        try {
          mServer.close();
        } catch (Exception e) {}
      }
    }
  }
  public int getPort() {
    return mPort;
  }
}
