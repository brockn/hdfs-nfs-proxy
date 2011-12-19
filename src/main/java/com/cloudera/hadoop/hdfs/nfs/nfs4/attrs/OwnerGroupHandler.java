package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;


import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;

public class OwnerGroupHandler extends AttributeHandler<OwnerGroup> {

  @Override
  public OwnerGroup get(NFS4Handler server, Session session, FileSystem fs,
      FileStatus fileStatus) {
    OwnerGroup ownerGroup = new OwnerGroup();
    String domain = OwnerHandler.getDomain(session.getConfiguration(), session.getClientHost());
    ownerGroup.setOwnerGroup(fileStatus.getGroup() + "@" + domain);
    return ownerGroup;
  }
  
  @Override
  public boolean set(NFS4Handler server, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, OwnerGroup attr) throws IOException {
    String group = OwnerHandler.removeDomain(attr.getOwnerGroup());
    if(fileStatus.getGroup().equals(group)) {
      fs.setOwner(fileStatus.getPath(), null, group);
      return true;
    }
    return false;
  }
}
