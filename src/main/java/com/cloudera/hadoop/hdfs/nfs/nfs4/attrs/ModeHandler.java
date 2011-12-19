package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;


import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;

public class ModeHandler extends AttributeHandler<Mode> {

  @Override
  public Mode get(NFS4Handler server, Session session,
      FileSystem fs, FileStatus fileStatus) {
    Mode mode = new Mode();
    mode.setMode(fileStatus.getPermission().toShort());
    return mode;
  }

  @Override
  public boolean set(NFS4Handler server, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, Mode attr) 
      throws NFS4Exception, IOException {
    FsPermission perm = new FsPermission((short)attr.getMode());
    fs.setPermission(fileStatus.getPath(), perm);
    return true;
  }
}
