package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Time;

public class CreateTimeHandler extends AttributeHandler<CreateTime> {

  @Override
  public CreateTime get(NFS4Handler server, Session session, FileSystem fs,
      FileStatus fileStatus) {
    CreateTime time = new CreateTime();
    time.setTime(new Time(fileStatus.getModificationTime())); // XXX bad fake?
    return time;
  }
  
}
