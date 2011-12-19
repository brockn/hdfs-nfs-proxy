package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.PUTROOTFHRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.PUTROOTFHResponse;

public class PUTROOTFHHandler extends OperationRequestHandler<PUTROOTFHRequest, PUTROOTFHResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(PUTROOTFHHandler.class);

  @Override
  protected PUTROOTFHResponse doHandle(NFS4Handler server, Session session,
      PUTROOTFHRequest request) throws NFS4Exception {
    session.setCurrentFileHandle(server.createFileHandle(new Path("/")));
    PUTROOTFHResponse response = createResponse();
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected PUTROOTFHResponse createResponse() {
    return new PUTROOTFHResponse();
  }

}
