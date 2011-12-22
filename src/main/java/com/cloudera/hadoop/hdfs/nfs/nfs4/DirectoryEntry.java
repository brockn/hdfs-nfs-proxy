package com.cloudera.hadoop.hdfs.nfs.nfs4;


import java.util.List;
import java.util.Map;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class DirectoryEntry implements WireSize {
  protected long mCookie;
  protected String mName;
  protected Bitmap mAttrs;
  protected ImmutableList<Attribute> mAttrValues;

  @Override
  public void read(RPCBuffer buffer) {
    mCookie = buffer.readUint64();
    mName = buffer.readString();
    Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.readAttrs(buffer);
    mAttrs = pair.getFirst();
    mAttrValues = pair.getSecond();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mCookie);
    buffer.writeString(mName);
    Attribute.writeAttrs(buffer, mAttrs, mAttrValues);
  }
  
  public Bitmap getAttrs() {
    return mAttrs;
  }

  public void setAttrs(Bitmap attrs) {
    this.mAttrs = attrs;
  }

  public ImmutableMap<Integer, Attribute> getAttrValues() {
    Map<Integer, Attribute> rtn = Maps.newHashMap();
    for(Attribute attr : mAttrValues) {
      rtn.put(attr.getID(), attr);
    }
    return ImmutableMap.copyOf(rtn);
  }

  public void setAttrValues(List<Attribute> attributes) {
    this.mAttrValues = ImmutableList.copyOf(attributes);
  }

  public long getCookie() {
    return mCookie;
  }

  public void setCookie(long cookie) {
    this.mCookie = cookie;
  }

  public String getName() {
    return mName;
  }

  public void setName(String name) {
    this.mName = name;
  }

  @Override
  public int getWireSize() {
    // TODO this is really bad but can be fixed later.
    RPCBuffer buffer = new RPCBuffer();
    write(buffer);
    buffer.flip();
    return buffer.length();
  }
  
  
}
