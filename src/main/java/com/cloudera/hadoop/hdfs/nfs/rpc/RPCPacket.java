package com.cloudera.hadoop.hdfs.nfs.rpc;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;

/**
 * Represents fields common to both RPCResponse and RPCRequest
 */
public abstract class RPCPacket implements MessageBase {
  protected static final Logger LOGGER = LoggerFactory.getLogger(RPCPacket.class);
  
  protected int mXid, mMessageType, mRpcVersion, mProgram, mProgramVersion, mProcedure;
  
  public void write(RPCBuffer buffer) {
    buffer.writeInt(Integer.MAX_VALUE); // save space
    buffer.writeInt(mXid);
    buffer.writeInt(mMessageType);
  }
  public void read(RPCBuffer buffer) {
    this.mXid = buffer.readInt();
    this.mMessageType = buffer.readInt();    
  }
   
  public int getXid() {
    return mXid;
  }
  public void setXid(int xid) {
    this.mXid = xid;
  }
  public int getMessageType() {
    return mMessageType;
  }
  public void setMessageType(int messageType) {
    this.mMessageType = messageType;
  }
  public int getRpcVersion() {
    return mRpcVersion;
  }
  public void setRpcVersion(int rpcVersion) {
    this.mRpcVersion = rpcVersion;
  }
  public int getProgram() {
    return mProgram;
  }
  public void setProgram(int program) {
    this.mProgram = program;
  }
  public int getProgramVersion() {
    return mProgramVersion;
  }
  public void setProgramVersion(int programVersion) {
    this.mProgramVersion = programVersion;
  }
  public int getProcedure() {
    return mProcedure;
  }
  public void setProcedure(int procedure) {
    this.mProcedure = procedure;
  }

  @Override
  public String toString() {
    return "RPCPacket [xid="
        + mXid + ", messageType=" + mMessageType + ", rpcVersion=" + mRpcVersion
        + ", program=" + mProgram + ", programVersion=" + mProgramVersion
        + ", procedure=" + mProcedure + "]";
  }
}
