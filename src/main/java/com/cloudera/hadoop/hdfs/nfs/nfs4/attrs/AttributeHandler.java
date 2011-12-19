package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;



import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;

public abstract class AttributeHandler<T extends Attribute> {
  
  public T get(NFS4Handler server, Session session, FileSystem fs, FileStatus fileStatus) throws NFS4Exception, IOException {
    throw new UnsupportedOperationException("Not implemented");
  }
  
  public boolean set(NFS4Handler server, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, T attr) 
      throws NFS4Exception, IOException {
    throw new UnsupportedOperationException("Not implemented " + attr.getID());
  }
}
