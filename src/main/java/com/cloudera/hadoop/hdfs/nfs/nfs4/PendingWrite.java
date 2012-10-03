package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.Bytes;

class PendingWrite {

  String name;
  int xid;
  long offset;
  boolean sync;
  byte[] data;
  int start;
  int length;
  int hashCode;
  int size;

  public PendingWrite(String name, int xid, long offset, boolean sync, byte[] data, int start, int length) {
    this.name = name;
    this.xid = xid;
    this.offset = offset;
    this.sync = sync;
    this.data = data;
    this.start = start;
    this.length = length;
    this.hashCode = hashCode();
    this.size = getSize();
  }

  public int getSize() {
    int size = 4; // obj header     
    size += name.length() + 4; // string, 4 byte length?
    size += 4; // xid
    size += 8; // offset
    size += 1; // sync
    size += data.length + 4; // data
    size += 4; // start
    size += 4; // length
    size += 4; // hashcode
    size += 4; // size
    return size;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (offset ^ (offset >>> 32));
    result = prime * result + hashBytes(data, start, length);
    return result;
  }

  protected static int hashBytes(byte[] bytes, int offset, int length) {
    int hash = 1;
    for (int i = offset; i < offset + length; i++) {
      hash = (31 * hash) + bytes[i];
    }
    return hash;
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
    PendingWrite other = (PendingWrite) obj;
    if (offset != other.offset) {
      return false;
    }
    return Bytes.compareTo(data, start, length, other.data, other.start, other.length) == 0;
  }
}