package com.cloudera.hadoop.hdfs.nfs.nfs4.requests;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;


public class READDIRRequest extends OperationRequest {
  protected long mCookie;
  protected OpaqueData8 mCookieVerifer;
  protected int mDirCount;
  protected int mMaxCount;  
  protected Bitmap mAttrs;
  @Override
  public void read(RPCBuffer buffer) {
    mCookie = buffer.readUint64();
    mCookieVerifer = new OpaqueData8();
    mCookieVerifer.read(buffer);
    mDirCount = buffer.readUint32();
    mMaxCount = buffer.readUint32();
    mAttrs = new Bitmap();
    mAttrs.read(buffer);
  }

  @Override
  public void write(RPCBuffer buffer) {
    buffer.writeUint64(mCookie);
    mCookieVerifer.write(buffer);
    buffer.writeUint32(mDirCount);
    buffer.writeUint32(mMaxCount);
    mAttrs.write(buffer);
  }
  
  @Override
  public int getID() {
    return NFS4_OP_READDIR;
  }

  public long getCookie() {
    return mCookie;
  }

  public void setCookie(long cookie) {
    this.mCookie = cookie;
  }

  public OpaqueData8 getCookieVerifer() {
    return mCookieVerifer;
  }

  public void setCookieVerifer(OpaqueData8 cookieVerifer) {
    this.mCookieVerifer = cookieVerifer;
  }

  public int getDirCount() {
    return mDirCount;
  }

  public void setDirCount(int dirCount) {
    this.mDirCount = dirCount;
  }

  public int getMaxCount() {
    return mMaxCount;
  }

  public void setMaxCount(int maxCount) {
    this.mMaxCount = maxCount;
  }

  public Bitmap getAttrs() {
    return mAttrs;
  }

  public void setAttrs(Bitmap attrs) {
    this.mAttrs = attrs;
  }
  
}
