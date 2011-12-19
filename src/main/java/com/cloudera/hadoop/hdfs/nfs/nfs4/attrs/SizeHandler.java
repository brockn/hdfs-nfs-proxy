package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;



import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;

public class SizeHandler extends AttributeHandler<Size> {

  @Override
  public Size get(NFS4Handler server, Session session, FileSystem fs,
      FileStatus fileStatus) throws NFS4Exception {
    Size size = new Size();
    size.setSize(server.getFileSize(fileStatus));
    return size;
  }
  /* Linux uses SETATTR (size = 0) to truncate files.
   */
  public boolean set(NFS4Handler server, Session session, FileSystem fs, FileStatus fileStatus, StateID stateID, Size size)
      throws NFS4Exception, IOException {
    // we only support truncating files
    if(size.getSize() != 0) {
      throw new UnsupportedOperationException("Setting size to non-zero (truncate) is not supported.");
    }
    // open the file, overwriting if needed. Creation of an empty file with
    // overwrite on is the only way we can support truncating files
    FSDataOutputStream out = server.forWrite(stateID, fs, session.getCurrentFileHandle(), true);
    if(out.getPos() != 0) {
      stateID = server.close(session.getSessionID(), stateID, stateID.getSeqID(), session.getCurrentFileHandle());
      out = server.forWrite(stateID, fs, session.getCurrentFileHandle(), true); 
    }
    out.sync();
    return true;
  }

}
