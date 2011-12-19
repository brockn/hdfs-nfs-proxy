package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.READRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.READResponse;

public class READHandler extends OperationRequestHandler<READRequest, READResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(READHandler.class);

  @Override
  protected READResponse doHandle(NFS4Handler server, Session session,
      READRequest request) throws NFS4Exception, IOException {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    int size = Math.min(request.getCount(), NFS4_MAX_RWSIZE);
    if(size < 0) {
      throw new NFS4Exception(NFS4ERR_INVAL);
    }
    FileHandle fileHandle = session.getCurrentFileHandle();
    Path path = server.getPath(fileHandle);
    FileSystem fs = session.getFileSystem();
    FSDataInputStream inputStream = server.forRead(request.getStateID(), fs, fileHandle);
    synchronized (inputStream) {
      if(inputStream.getPos() != request.getOffset()) {
        try {
          inputStream.seek(request.getOffset());
        } catch (IOException e) {
          throw new IOException(e.getMessage() + ": " + inputStream.getPos() + ", " + request.getOffset(), e);
        }
        server.incrementMetric("NFS_RANDOM_READS", 1);
      }
      READResponse response = createResponse();
      byte[] data = new byte[size];
      int count = inputStream.read(data);
      long fileLength = -1;
//      if(count == -1 && 
//          request.getOffset() < (fileLength = fs.getFileStatus(path).getLen())) {
//        count = inputStream.read(data);
//      }
      if(count != data.length) {
        LOGGER.info("Short read "+path+
            " at pos = " + request.getOffset() +
            ", wanted " + data.length + " and read " + count + 
            ", fileLength = " + fileLength);
      }
      boolean eof = count < 0;
      if(eof) {
        data = new byte[0];
        count = 0;
      }
      server.incrementMetric("HDFS_BYTES_READ", count);
      response.setData(data, 0, count);
      response.setEOF(eof);
      response.setStatus(NFS4_OK);
      return response;      
    }
  }

  @Override
  protected READResponse createResponse() {
    return new READResponse();
  }

}
