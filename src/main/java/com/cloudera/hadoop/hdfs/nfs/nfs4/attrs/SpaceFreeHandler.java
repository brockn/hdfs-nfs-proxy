package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import java.io.IOException;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;

public class SpaceFreeHandler extends AttributeHandler<SpaceFree> {

  @Override
  public SpaceFree get(NFS4Handler server, Session session,
      FileSystem fs, FileStatus fileStatus) throws NFS4Exception, IOException {
    SpaceFree space = new SpaceFree();
    space.set(fs.getStatus().getRemaining());
    return space;
  }

}
