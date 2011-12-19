package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Time;

public class ModifyTimeHandler extends AttributeHandler<ModifyTime> {

  @Override
  public ModifyTime get(NFS4Handler server, Session session, FileSystem fs,
      FileStatus fileStatus) {
    ModifyTime modifyTime = new ModifyTime();
    modifyTime.setTime(new Time(fileStatus.getModificationTime()));
    return modifyTime;
  }


}
