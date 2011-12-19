package com.cloudera.hadoop.hdfs.nfs.nfs4.responses;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.DirectoryList;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Status;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class READDIRResponse extends OperationResponse implements Status {
  
  protected int mStatus;
  protected OpaqueData8 mCookieVerifer;
  protected DirectoryList mDirectoryList;
  
  @Override
  public void read(RPCBuffer buffer) {
    reset();
    mStatus = buffer.readUint32();
    if(mStatus == NFS4_OK) {
      mCookieVerifer = new OpaqueData8();
      mCookieVerifer.read(buffer);
      mDirectoryList = new DirectoryList();
      mDirectoryList.read(buffer);
    }
  }
  
  protected void reset() {
    mCookieVerifer = null;
    mDirectoryList = null;
  }
  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mStatus);
    if(mStatus == NFS4_OK) {
      mCookieVerifer.write(buffer);
      mDirectoryList.write(buffer);
    }
  }

  @Override
  public int getStatus() {
    return mStatus;
  }
  @Override
  public void setStatus(int status) {
    this.mStatus = status;
  }

  @Override
  public int getID() {
    return NFS4_OP_READDIR;
  }

  public OpaqueData8 getCookieVerifer() {
    return mCookieVerifer;
  }

  public void setCookieVerifer(OpaqueData8 cookieVerifer) {
    this.mCookieVerifer = cookieVerifer;
  }

  public DirectoryList getDirectoryList() {
    return mDirectoryList;
  }

  public void setDirectoryList(DirectoryList directoryList) {
    this.mDirectoryList = directoryList;
  }
  
}
