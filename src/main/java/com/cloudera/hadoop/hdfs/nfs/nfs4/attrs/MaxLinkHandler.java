package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;

public class MaxLinkHandler extends AttributeHandler<MaxLink> {

  @Override
  public MaxLink get(NFS4Handler server, Session session,
      FileSystem fs, FileStatus fileStatus) throws NFS4Exception {
    MaxLink max = new MaxLink();
    max.setValue(1);
    return max;
  }

}
