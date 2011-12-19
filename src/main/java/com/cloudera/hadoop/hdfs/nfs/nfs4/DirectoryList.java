package com.cloudera.hadoop.hdfs.nfs.nfs4;


import java.util.List;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class DirectoryList implements MessageBase {

  protected ImmutableList<DirectoryEntry> mDirEntries = ImmutableList.of();
  protected boolean mEOF;
  @Override
  public void read(RPCBuffer buffer) {
    List<DirectoryEntry> entries = Lists.newArrayList();
    while(buffer.readBoolean()) {
      DirectoryEntry entry = new DirectoryEntry();
      entry.read(buffer);
      entries.add(entry);
    }
    mDirEntries = ImmutableList.copyOf(entries);
    mEOF = buffer.readBoolean();
  }
  @Override
  public void write(RPCBuffer buffer) {
    for(DirectoryEntry entry : mDirEntries) {
      buffer.writeBoolean(true);
      entry.write(buffer);
    }
    buffer.writeBoolean(false);
    buffer.writeBoolean(mEOF);
  }
  public ImmutableList<DirectoryEntry> getDirEntries() {
    return mDirEntries;
  }
  public void setDirEntries(List<DirectoryEntry> dirEntries) {
    this.mDirEntries = ImmutableList.copyOf(dirEntries);
  }
  public boolean isEOF() {
    return mEOF;
  }
  public void setEOF(boolean EOF) {
    this.mEOF = EOF;
  }
  
  
}
