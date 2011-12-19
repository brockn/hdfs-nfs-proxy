package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.COMMITRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.COMMITResponse;

public class COMMITHandler extends OperationRequestHandler<COMMITRequest, COMMITResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(COMMITHandler.class);

  @Override
  protected COMMITResponse doHandle(NFS4Handler server, Session session,
      COMMITRequest request) throws NFS4Exception, IOException {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    WriteOrderHandler writeOrderHandler = server.forCommit(session.getFileSystem(), session.getCurrentFileHandle());
    long offset = request.getOffset() + request.getCount();
    if(offset == 0) {
      offset = writeOrderHandler.getCurrentPos();
    }
    writeOrderHandler.sync(offset);
    COMMITResponse response = createResponse();
    OpaqueData8 verifer = new OpaqueData8();
    verifer.setData(Bytes.toBytes(server.getStartTime()));
    response.setVerifer(verifer);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected COMMITResponse createResponse() {
    return new COMMITResponse();
  }

}
