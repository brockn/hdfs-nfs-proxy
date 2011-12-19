package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.ChangeInfo;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.ChangeID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.OPENRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.OPENResponse;

public class OPENHandler extends
    OperationRequestHandler<OPENRequest, OPENResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(OPENHandler.class);

  @Override
  protected OPENResponse doHandle(NFS4Handler server, Session session,
      OPENRequest request) throws NFS4Exception, IOException, UnsupportedOperationException {
    if (session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    switch (request.getAccess()) {
      case NFS4_OPEN4_SHARE_ACCESS_READ:
        return read(server, session, request);
      case NFS4_OPEN4_SHARE_ACCESS_WRITE:
        return write(server, session, request);
      default:
        throw new NFS4Exception(NFS4ERR_NOTSUPP,"read OR write not both ", true);
    }
  }

  protected OPENResponse read(NFS4Handler server, Session session,
      OPENRequest request) throws NFS4Exception, IOException {
    if(request.getDeny() != 0) {
      throw new UnsupportedOperationException("Read access does not support deny " + request.getDeny());
    }
    // generate stateid
    StateID stateID = StateID.newStateID(request.getSeqID());
    FileSystem fs = session.getFileSystem();
    Path parentPath = server.getPath(session.getCurrentFileHandle());
    Path path = new Path(parentPath, request.getName());
    session.setCurrentFileHandle(server.createFileHandle(path));
    @SuppressWarnings("unused")
    FSDataInputStream in = server.forRead(stateID, fs, session.getCurrentFileHandle());
    OPENResponse response = createResponse();
    response.setStateID(stateID);
    FileStatus fileStatus = fs.getFileStatus(path);
    // TODO this is  wrong but files in HDFS are currently immutable once closed
    ChangeID changeID = new ChangeID();
    changeID.setChangeID(fileStatus.getModificationTime());
    ChangeInfo changeInfo = new ChangeInfo();
    changeInfo.setChangeIDBefore(changeID);
    changeInfo.setChangeIDAfter(changeID);
    changeInfo.setAtomic(true);
    response.setChangeID(changeInfo);
    response.setResultFlags(NFS4_OPEN4_RESULT_CONFIRM); // TODO do we really need confirm step?
//    if(request.getAttrs() != null) {
//      Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.getAttrs(server, session, request.getAttrs(), fs, fileStatus);
//      response.setAttrs(pair.getFirst());
//      response.setAttrValues(pair.getSecond());
//    }
    response.setDelgationType(NFS4_CLAIM_NULL);
    response.setStatus(NFS4_OK);
    return response;
  }

  protected OPENResponse write(NFS4Handler server, Session session,
      OPENRequest request) throws NFS4Exception, IOException {
    // generate stateid
    StateID stateID = StateID.newStateID(request.getSeqID());
    FileSystem fs = session.getFileSystem();
    Path parentPath = server.getPath(session.getCurrentFileHandle());
    Path path = new Path(parentPath, request.getName());
    session.setCurrentFileHandle(server.createFileHandle(path));
    boolean overwrite = request.getOpenType() == NFS4_OPEN4_CREATE;
    FSDataOutputStream out = server.forWrite(stateID, fs, session.getCurrentFileHandle(), overwrite);
    out.sync(); // create file in namenode
    OPENResponse response = createResponse();
    response.setStateID(stateID);
    // TODO this is wrong but files in HDFS are currently immutable once closed
    ChangeID changeID = new ChangeID();
    changeID.setChangeID(System.currentTimeMillis());
    ChangeInfo changeInfo = new ChangeInfo();
    changeInfo.setChangeIDBefore(changeID);
    changeInfo.setChangeIDAfter(changeID);
    changeInfo.setAtomic(true);
    response.setChangeID(changeInfo);
    response.setResultFlags(NFS4_OPEN4_RESULT_CONFIRM); // TODO do we really need confirm step?
    // TODO should be setattr or getattr here? Need to read RFC
    if(request.getAttrs() != null) {
//      Attribute.setAttrs(server, session, request.getAttrs(), request.getAttrValues(), fs, fileStatus, stateID);
//      Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.getAttrs(server, session, request.getAttrs(), fs, fileStatus);
//      response.setAttrs(pair.getFirst());
//      response.setAttrValues(pair.getSecond());
    }
    response.setDelgationType(NFS4_CLAIM_NULL);
    response.setStatus(NFS4_OK);
    return response;
  }

  @Override
  protected OPENResponse createResponse() {
    return new OPENResponse();
  }

}
