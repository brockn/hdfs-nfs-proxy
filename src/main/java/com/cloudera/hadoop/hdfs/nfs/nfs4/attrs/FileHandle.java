package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
public class FileHandle extends Attribute {
  public FileHandle() {
    super();
  }
  protected byte[] mFileHandle;
  @Override
  public void read(RPCBuffer buffer) {
    mFileHandle = buffer.readBytes();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mFileHandle.length);
    buffer.writeBytes(mFileHandle);
  }

  @Override
  public int getID() {
    return NFS4_FATTR4_FILEHANDLE;
  }

  public byte[] getFileHandle() {
    return mFileHandle;
  }

  public void set(byte[] fileHandle) {
    this.mFileHandle = fileHandle;
  }
  
}
