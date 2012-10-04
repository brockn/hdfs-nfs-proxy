/**
 * Copyright 2012 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DFSClient;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.google.common.collect.Maps;

public class FSInfo {

  private static boolean useDFSClient = false;
  private static final Map<Configuration, BlockingQueue<DFSClient>> clients = Maps.newHashMap();
  
  static {
    try {
      FileSystem.class.getMethod("getStatus", (Class[])null);
      useDFSClient = false;
    } catch (NoSuchMethodException e) {
      useDFSClient = true;
    }
    try {
      if(useDFSClient) {
        DFSClient.class.getMethod("totalRawCapacity", (Class[])null);
        DFSClient.class.getMethod("totalRawUsed", (Class[])null);
      } else {
        Class.forName("org.apache.hadoop.fs.FsStatus").getMethod("getCapacity", (Class[])null);
        Class.forName("org.apache.hadoop.fs.FsStatus").getMethod("getUsed", (Class[])null);
      }
    } catch(Exception ex) {
      throw new RuntimeException("The version of hadoop you have is not supported", ex);
    }
  }
  
   
  public long getCapacity(Session session) throws IOException {
    if(session.getFileSystem() instanceof LocalFileSystem) {
      File partition = new File("/");
      return partition.getTotalSpace();
    }
    if(useDFSClient) {
      DFSClient client = getDFSClient(session.getConfiguration());
      try {
        return (Long)getObject(client, "totalRawCapacity");
      } finally {
        putDFSClient(session.getConfiguration(), client);
      }
    }
    FileSystem fs = session.getFileSystem();
    return (Long)getObject(getObject(fs, "getStatus"), "getCapacity");
  }
  public long getRemaining(Session session)  throws IOException{
    return getCapacity(session) - getUsed(session);
  }
  public long getUsed(Session session)  throws IOException {
    if(session.getFileSystem() instanceof LocalFileSystem) {
      File partition = new File("/");
      return partition.getTotalSpace() - partition.getFreeSpace();
    }
    if(useDFSClient) {
      DFSClient client = getDFSClient(session.getConfiguration());
      try {
        return (Long)getObject(client, "totalRawUsed");
      } finally {
        putDFSClient(session.getConfiguration(), client);
      }
    }
    FileSystem fs = session.getFileSystem();
    return (Long)getObject(getObject(fs, "getStatus"), "getUsed");
  }

  private static void putDFSClient(Configuration conf, DFSClient client) 
      throws IOException {
    BlockingQueue<DFSClient> clientQueue;
    synchronized(clients) {
      clientQueue = clients.get(conf);
      if(clientQueue == null) {
        clientQueue = new ArrayBlockingQueue<DFSClient>(1);
        clients.put(conf, clientQueue);
      }      
    }
    if(!clientQueue.offer(client)) {
      client.close();
    }
  }
  private static DFSClient getDFSClient(Configuration conf) 
      throws IOException {
    BlockingQueue<DFSClient> clientQueue;
    synchronized(clients) {
      clientQueue = clients.get(conf);
      if(clientQueue == null) {
        clientQueue = new ArrayBlockingQueue<DFSClient>(1);
        clients.put(conf, clientQueue);
      }      
    }
    DFSClient client = clientQueue.poll();
    if(client == null) {
      client = new DFSClient(conf);
    }
    return client;
  }

  protected static Object getObject(Object obj, String name) {
    try {
      Method method = obj.getClass().getMethod(name, (Class[])null);
      return method.invoke(obj, (Object[])null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
