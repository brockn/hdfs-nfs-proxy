package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Time;

public class MetadataTimeHandler extends AttributeHandler<MetadataTime> {

  @Override
  public MetadataTime get(NFS4Handler server, Session session, FileSystem fs,
      FileStatus fileStatus) {
    MetadataTime time = new MetadataTime();
    time.setTime(new Time(fileStatus.getModificationTime()));
    return time;
  }


}
