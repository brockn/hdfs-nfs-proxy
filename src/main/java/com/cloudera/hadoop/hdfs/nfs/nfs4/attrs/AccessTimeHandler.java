package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Time;

public class AccessTimeHandler extends AttributeHandler<AccessTime> {

  @Override
  public AccessTime get(NFS4Handler server, Session session, FileSystem fs,
      FileStatus fileStatus) {
    AccessTime accessTime = new AccessTime();
    accessTime.setTime(new Time(fileStatus.getAccessTime()));
    return accessTime;
  }
  
}
