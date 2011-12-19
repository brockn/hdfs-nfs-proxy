package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;


import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;

public class TypeHandler extends AttributeHandler<Type> {

  @Override
  public Type get(NFS4Handler server, Session session, FileSystem fs,
      FileStatus fileStatus) throws NFS4Exception {
    Type type = new Type();
    if(fileStatus.isDir()) {
      type.setType(NFS4_DIR);
    } else {
      type.setType(NFS4_REG);      
    }
    return type;
  }

}
