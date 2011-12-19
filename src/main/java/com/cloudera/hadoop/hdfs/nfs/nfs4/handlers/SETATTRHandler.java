package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.SETATTRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.SETATTRResponse;
import com.google.common.collect.ImmutableMap;



public class SETATTRHandler extends OperationRequestHandler<SETATTRRequest, SETATTRResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(SETATTRHandler.class);

  @Override
  protected SETATTRResponse doHandle(NFS4Handler server, Session session,
      SETATTRRequest request) throws NFS4Exception, IOException {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    Path path = server.getPath(session.getCurrentFileHandle());
    FileSystem fs = session.getFileSystem();
    FileStatus fileStatus = fs.getFileStatus(path);
    ImmutableMap<Integer, Attribute> requestAttrs = request.getAttrValues();
    Bitmap responseAttrs = Attribute.setAttrs(server, session, 
        request.getAttrs(), requestAttrs, fs, fileStatus, request.getStateID());
    SETATTRResponse response = createResponse();
    response.setStatus(NFS4_OK);
    response.setAttrs(responseAttrs);
    return response;
  }
    @Override
  protected SETATTRResponse createResponse() {
    return new SETATTRResponse();
  }
}
