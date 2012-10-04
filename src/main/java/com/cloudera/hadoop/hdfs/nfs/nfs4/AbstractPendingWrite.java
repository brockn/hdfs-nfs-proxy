package com.cloudera.hadoop.hdfs.nfs.nfs4;


abstract class AbstractPendingWrite implements PendingWrite {

  private final String name;
  private final int xid;
  private final long offset;
  private final boolean sync;

  public AbstractPendingWrite(String name, int xid, long offset, boolean sync) {
    this.name = name;
    this.xid = xid;
    this.offset = offset;
    this.sync = sync;
  }
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getXidAsHexString() {
    return Integer.toHexString(xid);
  }
  @Override
  public int getXid() {
    return xid;
  }
  @Override
  public long getOffset() {
    return offset;
  }
  @Override
  public boolean isSync() {
    return sync;
  }
  protected static int getHashCode(long offset, byte[] data, int start, int length) {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (offset ^ (offset >>> 32));
    result = prime * result + hashBytes(data, start, length);
    return result;
  }
  private static int hashBytes(byte[] bytes, int offset, int length) {
    int hash = 1;
    for (int i = offset; i < offset + length; i++) {
      hash = (31 * hash) + bytes[i];
    }
    return hash;
  }

}