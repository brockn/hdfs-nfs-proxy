package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class FileID extends Attribute {
  public FileID() {
    super();
  }
  protected long mID;
  @Override
  public void read(RPCBuffer buffer) {
    mID = buffer.readUint64();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mID);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_FILEID;
  }

  public long getFileID() {
    return mID;
  }

  public void setFileID(long id) {
    this.mID = id;
  }
  
}
