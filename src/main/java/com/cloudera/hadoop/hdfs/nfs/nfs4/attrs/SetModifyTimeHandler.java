package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;

public class SetModifyTimeHandler extends AttributeHandler<SetModifyTime> {

  @Override
  public boolean set(NFS4Handler server, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, SetModifyTime attr) 
      throws NFS4Exception, IOException {
    if(attr.getHow() == NFS4_SET_TO_CLIENT_TIME4) {
      fs.setTimes(fileStatus.getPath(), attr.getTime().toMilliseconds(), fileStatus.getAccessTime());
    } else {
      fs.setTimes(fileStatus.getPath(), System.currentTimeMillis(), fileStatus.getAccessTime());
    }
    return true;
  }

}
