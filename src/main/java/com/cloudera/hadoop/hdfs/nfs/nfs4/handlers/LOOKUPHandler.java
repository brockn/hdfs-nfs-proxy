package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.LOOKUPRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.LOOKUPResponse;

public class LOOKUPHandler extends OperationRequestHandler<LOOKUPRequest, LOOKUPResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(LOOKUPHandler.class);

  @Override
  protected LOOKUPResponse doHandle(NFS4Handler server, Session session,
      LOOKUPRequest request) throws NFS4Exception, IOException {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    if("".equals(request.getName())) {
      throw new NFS4Exception(NFS4ERR_INVAL);
    }
    Path parentPath = server.getPath(session.getCurrentFileHandle());
    Path path = new Path(parentPath, request.getName());
    FileSystem fs = session.getFileSystem();
    if(!server.fileExists(fs, path)) {
      throw new NFS4Exception(NFS4ERR_NOENT, "Path " + path + " does not exist.", true);
    }
    LOOKUPResponse response = createResponse();
    FileHandle fileHandle = server.createFileHandle(path);
    session.setCurrentFileHandle(fileHandle);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected LOOKUPResponse createResponse() {
    return new LOOKUPResponse();
  }

}
