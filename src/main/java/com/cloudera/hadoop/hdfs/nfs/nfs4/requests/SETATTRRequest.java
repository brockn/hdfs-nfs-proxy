package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.util.List;
import java.util.Map;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class SETATTRRequest extends OperationRequest {

  protected StateID mStateID;
  protected Bitmap mAttrs;
  protected ImmutableList<Attribute> mAttrValues;
  @Override
  public void read(RPCBuffer buffer) {
    mStateID = new StateID();
    mStateID.read(buffer);
    Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.readAttrs(buffer);
    mAttrs = pair.getFirst();
    mAttrValues = pair.getSecond();
  }

  @Override
  public void write(RPCBuffer buffer) {
    mStateID.write(buffer);
    Attribute.writeAttrs(buffer, mAttrs, mAttrValues);
  }
  
  public StateID getStateID() {
    return mStateID;
  }

  public void setmStateID(StateID stateID) {
    this.mStateID = stateID;
  }

  @Override
  public int getID() {
    return NFS4_OP_SETATTR;
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
