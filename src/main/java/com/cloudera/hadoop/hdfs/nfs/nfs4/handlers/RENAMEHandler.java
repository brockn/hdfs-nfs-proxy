package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.ChangeInfo;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.RENAMERequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.RENAMEResponse;

public class RENAMEHandler extends OperationRequestHandler<RENAMERequest, RENAMEResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RENAMEHandler.class);

  @Override
  protected RENAMEResponse doHandle(NFS4Handler server, Session session,
      RENAMERequest request) throws NFS4Exception, IOException {
    if(session.getCurrentFileHandle() == null || session.getSavedFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    if("".equals(request.getOldName()) || "".equals(request.getNewName())) {
      throw new NFS4Exception(NFS4ERR_INVAL);
    }
    FileSystem fs = session.getFileSystem();
    Path oldParentPath = server.getPath(session.getSavedFileHandle());
    Path oldPath = new Path(oldParentPath, request.getOldName());
    Path newParentPath = server.getPath(session.getCurrentFileHandle());
    Path newPath = new Path(newParentPath, request.getNewName());
    if(!(fs.getFileStatus(oldParentPath).isDir() && fs.getFileStatus(newParentPath).isDir())) {
      throw new NFS4Exception(NFS4ERR_NOTDIR);
    }    
    if(!server.fileExists(fs, oldPath)) {
      throw new NFS4Exception(NFS4ERR_NOENT, "Path " + oldPath + " does not exist.");
    }
    if(server.fileExists(fs, newPath)) {
      // TODO according to the RFC we are supposed to check to see if 
      // the entry which exists is compatible (overwrite file and 
      // empty directory if the "old" item is a file or dir respectively.
      throw new NFS4Exception(NFS4ERR_EXIST, "Path " + newPath + " exists.");
    }
    // we won't support renaming files which are open
    if(server.isFileOpen(oldPath)) {
      throw new NFS4Exception(NFS4ERR_FILE_OPEN);
    }
    LOGGER.info(session.getSessionID() + " Renaming " + oldPath + " to " + newPath);
    long beforeSource = fs.getFileStatus(oldParentPath).getModificationTime();
    long beforeDest = fs.getFileStatus(newParentPath).getModificationTime();
    if(!fs.rename(oldPath, newPath)) {
      throw new NFS4Exception(NFS4ERR_IO);
    }
    long afterSource = fs.getFileStatus(oldParentPath).getModificationTime();
    long afterDest = fs.getFileStatus(newParentPath).getModificationTime();
    RENAMEResponse response = createResponse();
    response.setChangeInfoSource(ChangeInfo.newChangeInfo(true, beforeSource, afterSource));
    response.setChangeInfoDest(ChangeInfo.newChangeInfo(true, beforeDest, afterDest));
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected RENAMEResponse createResponse() {
    return new RENAMEResponse();
  }

}
