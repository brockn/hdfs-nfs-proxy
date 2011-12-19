package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OperationFactory;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class CompoundRequest implements MessageBase, RequiresCredentials {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CompoundRequest.class);
  protected int mMinorVersion;
  protected byte[] mTags = new byte[0];
  protected AuthenticatedCredentials mCredentials;
  
  protected ImmutableList<OperationRequest> mOperations = ImmutableList.<OperationRequest>builder().build();
  public CompoundRequest() {
    
  }
  
  @Override
  public void read(RPCBuffer buffer) {
    mTags = buffer.readBytes();
    mMinorVersion = buffer.readUint32();
    int count = buffer.readUint32();
    List<OperationRequest> ops = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      int id = buffer.readUint32();
      if(OperationFactory.isSupported(id)) {
        ops.add(OperationFactory.parseRequest(buffer, id));        
      } else {
        LOGGER.warn("Dropping request with id " + id + ": " + ops);
        throw new UnsupportedOperationException("NFS ID " + id);
      }
    }
    mOperations = ImmutableList.<OperationRequest>builder().addAll(ops).build();
  }
  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mTags.length);
    buffer.writeBytes(mTags);
    buffer.writeUint32(mMinorVersion);
    buffer.writeUint32(mOperations.size());
    for(OperationRequest operation : mOperations) {
      buffer.writeUint32(operation.getID());
      operation.write(buffer);
    }
  }
  public int getMinorVersion() {
    return mMinorVersion;
  }
  public void setMinorVersion(int mMinorVersion) {
    this.mMinorVersion = mMinorVersion;
  }
  public void setOperations(List<OperationRequest> operations) {
    mOperations = ImmutableList.<OperationRequest>copyOf(operations);
  }
  public ImmutableList<OperationRequest> getOperations() {
    return mOperations;
  }
  
  @Override
  public AuthenticatedCredentials getCredentials() {
    return mCredentials;
  }

  @Override
  public void setCredentials(AuthenticatedCredentials mCredentials) {
    this.mCredentials = mCredentials;
  }

  @Override
  public String toString() {
    return mOperations.toString();
  }

  public static CompoundRequest from(RPCBuffer buffer) {
    CompoundRequest request = new CompoundRequest();
    request.read(buffer);
    return request;
  }
}