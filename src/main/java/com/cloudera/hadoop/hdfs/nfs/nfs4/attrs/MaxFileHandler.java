package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;

public class MaxFileHandler extends AttributeHandler<MaxFile> {

  @Override
  public MaxFile get(NFS4Handler server, Session session,
      FileSystem fs, FileStatus fileStatus) throws NFS4Exception {
    MaxFile max = new MaxFile();
    max.setSize(NFS4_MAX_FILE_SIZE);
    return max;
  }

}
