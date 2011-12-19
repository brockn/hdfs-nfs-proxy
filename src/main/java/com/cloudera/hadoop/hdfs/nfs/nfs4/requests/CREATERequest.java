package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.PathUtils.*;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.util.List;
import java.util.Map;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


public class CREATERequest extends OperationRequest {

  protected int mType;
  protected String mName;
  protected Bitmap mAttrs;
  protected ImmutableList<Attribute> mAttrValues;
  @Override
  public void read(RPCBuffer buffer) {
    mType = buffer.readUint32();
    mName = checkPath(buffer.readString());
    Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.readAttrs(buffer);
    mAttrs = pair.getFirst();
    mAttrValues = pair.getSecond();
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint32(mType);
    buffer.writeString(mName);
    Attribute.writeAttrs(buffer, mAttrs, mAttrValues);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_CREATE;
  }
  
  public String getName() {
    return mName;
  }
  public void setName(String name) {
    mName = name;
  }
  public int getType() {
    return mType;
  }
  public void setType(int type) {
    mType = type;
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
}
