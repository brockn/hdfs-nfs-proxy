package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DFSClient;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;

public class FSInfo {

  protected static boolean useDFSClient = false;
  
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
  public static long getCapacity(Session session) throws IOException {
    if(session.getFileSystem() instanceof LocalFileSystem) {
      File partition = new File("/");
      return partition.getTotalSpace();
    }
    if(useDFSClient) {
      DFSClient client = new DFSClient(session.getConfiguration());
      try {
        return (Long)getObject(client, "totalRawCapacity");
      } finally {
        client.close();
      }
    }
    FileSystem fs = session.getFileSystem();
    return (Long)getObject(getObject(fs, "getStatus"), "getCapacity");
  }
  public static long getRemaining(Session session)  throws IOException{
    return getCapacity(session) - getUsed(session);
  }
  public static long getUsed(Session session)  throws IOException {
    if(session.getFileSystem() instanceof LocalFileSystem) {
      File partition = new File("/");
      return partition.getTotalSpace() - partition.getFreeSpace();
    }
    if(useDFSClient) {
      DFSClient client = new DFSClient(session.getConfiguration());
      try {
        return (Long)getObject(client, "totalRawUsed");
      } finally {
        client.close();
      }
    }
    FileSystem fs = session.getFileSystem();
    return (Long)getObject(getObject(fs, "getStatus"), "getUsed");
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
