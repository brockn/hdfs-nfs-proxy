package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.GETATTRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.GETATTRResponse;
import com.google.common.collect.ImmutableList;

public class GETATTRHandler extends OperationRequestHandler<GETATTRRequest, GETATTRResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(GETATTRHandler.class);

  @Override
  protected GETATTRResponse doHandle(NFS4Handler server, Session session,
      GETATTRRequest request) throws NFS4Exception, IOException {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    Path path = server.getPath(session.getCurrentFileHandle());
    try {
       FileSystem fs = session.getFileSystem();
       FileStatus fileStatus = fs.getFileStatus(path);
       Pair<Bitmap, ImmutableList<Attribute>> attrs = Attribute.getAttrs(server, session, 
           request.getAttrs(), fs, fileStatus);
       GETATTRResponse response = createResponse();
       response.setStatus(NFS4_OK);
       response.setAttrs(attrs.getFirst());
       response.setAttrValues(attrs.getSecond());
       return response;
    } catch(FileNotFoundException e) {
      throw new NFS4Exception(NFS4ERR_NOENT);
    }
  }
    @Override
  protected GETATTRResponse createResponse() {
    return new GETATTRResponse();
  }
}
