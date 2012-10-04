package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.Bytes;

public class MemoryBackedWrite extends AbstractPendingWrite {
  final byte[] data;
  final int start;
  final int length;
  final int hashCode;
  final int size;
  
  public MemoryBackedWrite(String name, int xid, long offset, boolean sync,
      byte[] data, int start, int length) {
    super(name, xid, offset, sync);
    this.data = data;
    this.start = start;
    this.length = length;
    this.hashCode = getHashCode(offset, data, start, length);
    this.size = getSize(name, length);
  }
  @Override
  public int getSize() {
    return size;
  }
  @Override
  public int hashCode() {
    return hashCode;
  }
  @Override
  public byte[] getData() {
    return data;
  }
  @Override
  public int getStart() {
    return start;
  }
  @Override
  public int getLength() {
    return length;
  }
  private static int getSize(String name, int dataLength) {
    int size = 4; // obj header     
    size += name.length() + 4; // string, 4 byte length?
    size += 4; // xid
    size += 8; // offset
    size += 1; // sync
    size += dataLength; // data
    size += 4; // start
    size += 4; // length
    size += 4; // hashcode
    size += 4; // size
    return size;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MemoryBackedWrite other = (MemoryBackedWrite) obj;
    if (getOffset() != other.getOffset()) {
      return false;
    }
    return Bytes.compareTo(data, start, length, other.data, other.start, other.length) == 0;
  }
}
