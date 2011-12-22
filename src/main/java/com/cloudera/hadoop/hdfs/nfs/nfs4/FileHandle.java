package com.cloudera.hadoop.hdfs.nfs.nfs4;


import java.util.Arrays;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

/**
 * Represents a NFS FileHandle.
 */
public class FileHandle implements MessageBase {
  protected byte[] mBytes;
  public FileHandle() {
    
  }
  public FileHandle(byte[] bytes) {
    this.mBytes = bytes;
  }
  @Override
  public void read(RPCBuffer buffer) {
    mBytes = buffer.readBytes();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mBytes.length);
    buffer.writeBytes(mBytes);
  }

  public byte[] getBytes() {
    return mBytes;
  }

  public void setBytes(byte[] bytes) {
    this.mBytes = bytes;
  }

  @Override
  public String toString() {
    return "FileHandle [mBytes=" + Arrays.toString(mBytes) + "]";
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(mBytes);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FileHandle other = (FileHandle) obj;
    if (!Arrays.equals(mBytes, other.mBytes))
      return false;
    return true;
  }
}
