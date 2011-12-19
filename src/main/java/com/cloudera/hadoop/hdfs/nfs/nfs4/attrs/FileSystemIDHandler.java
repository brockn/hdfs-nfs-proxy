package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;

public class FileSystemIDHandler extends AttributeHandler<FileSystemID> {

  @Override
  public FileSystemID get(NFS4Handler server, Session session,
      FileSystem fs, FileStatus fileStatus) {
    FileSystemID fsSystemID = new FileSystemID();
    fsSystemID.setMajor(0L);
    fsSystemID.setMinor(0L);
    return fsSystemID;
  }

}
