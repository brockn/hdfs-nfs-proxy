package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.ChangeID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class ChangeInfo implements MessageBase {

  protected boolean mAtomic; // XXX boolean?
  protected ChangeID mChangeIDBefore;
  protected ChangeID mChangeIDAfter;
  

  @Override
  public void read(RPCBuffer buffer) {
    mAtomic = buffer.readBoolean();
    mChangeIDBefore = new ChangeID();
    mChangeIDBefore.read(buffer);
    mChangeIDAfter = new ChangeID();
    mChangeIDAfter.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) { 
    buffer.writeBoolean(mAtomic);
    mChangeIDBefore.write(buffer);
    mChangeIDAfter.write(buffer);
  }


  public boolean getAtomic() {
    return mAtomic;
  }

  public void setAtomic(boolean atomic) {
    this.mAtomic = atomic;
  }

  public ChangeID getChangeIDBefore() {
    return mChangeIDBefore;
  }

  public void setChangeIDBefore(ChangeID changeIDBefore) {
    this.mChangeIDBefore = changeIDBefore;
  }

  public ChangeID getChangeIDAfter() {
    return mChangeIDAfter;
  }

  public void setChangeIDAfter(ChangeID changeIDAfter) {
    this.mChangeIDAfter = changeIDAfter;
  }

  public static ChangeInfo newChangeInfo(boolean atomic, long before, long after) {
    ChangeInfo changeInfo = new ChangeInfo();
    
    changeInfo.setAtomic(atomic);
    ChangeID changeIDBefore = new ChangeID();
    changeIDBefore.setChangeID(before);
    changeInfo.setChangeIDBefore(changeIDBefore);
    
    ChangeID changeIDAfter = new ChangeID();
    changeIDAfter.setChangeID(after);
    changeInfo.setChangeIDAfter(changeIDAfter);
    return changeInfo;
  }
}
