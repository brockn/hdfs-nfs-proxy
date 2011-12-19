package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;


import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;

public class OwnerHandler extends AttributeHandler<Owner> {

  @Override
  public Owner get(NFS4Handler server, Session session, FileSystem fs,
      FileStatus fileStatus) {
    Owner owner = new Owner();
    String domain = getDomain(session.getConfiguration(), session.getClientHost());
    owner.setOwner(fileStatus.getOwner() + "@" + domain);
    return owner;
  }
  
  @Override
  public boolean set(NFS4Handler server, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, Owner attr) 
      throws IOException {
    String user = removeDomain(attr.getOwner());
    if(fileStatus.getOwner().equals(user)) {
      fs.setOwner(fileStatus.getPath(), user, null);
      return true;
    }
    return false;
  }

  public static String removeDomain(String user) {
    int pos;
    if((pos = user.indexOf('@')) > 0 && pos < user.length()) {
      return user.substring(pos + 1);
    }
    return user;
  }
  public static String getDomain(Configuration conf, String host) {
    String override = conf.get(NFS_OWNER_DOMAIN);
    if(override != null) {
      return override;
    }
    int pos;
    if((pos = host.indexOf('.')) > 0 && pos < host.length()) {
      return host.substring(pos + 1);
    }
    return host;
  }

}
