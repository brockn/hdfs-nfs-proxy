package com.cloudera.hadoop.hdfs.nfs.nfs4;

import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

/**
 * Messages in our case are byte arrays. They need to 
 * deconstructed (read) from left to right. Wrapper by 
 * wrapper.
 * 
 * RPC.readPart() -> NFS.readPart() -> NFS Data.readPart()
 * 
 * As such, we use the readPart method as we processing
 * the message. Each layer down in the stack is responsible
 * for reading it's portion of the message.
 * 
 * Writing messages is similar. We need to write the left
 * most portion first and then continue right. 
 * 
 * RPC.write() -> NFS.write() -> NFS Data.write()
 * 
 * In our implementation the ApplicationMessage is the
 * unit we pass around. It will first all super.write(buffer)
 * to serialized the RPC portion, write itself out, and then
 * serialized any data it may have. 
 * 
 * This ApplicationMessage could be request/response but
 * is always for NFS/Mountd/PortMap/etc
 * 
 * Right before sending the message we call 
 * RPCBuffer.writeBufferSize to update the size of the 
 * buffer we which is bytes 1-7 in our case.
 */
public interface MessageBase {
  public void read(RPCBuffer buffer);
  public void write(RPCBuffer buffer);
}
