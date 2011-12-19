package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.TestUtils.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Time;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.ChangeID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.FileID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.FileSystemID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Mode;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.ModifyTime;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Owner;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.OwnerGroup;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Size;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Type;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;

public class TestAttrs {
  
  
  @Test
  public void testChangeID() throws Exception {
    ChangeID base = new ChangeID();
    base.setChangeID(5L);
    ChangeID copy = new ChangeID();
    testAttribute(base, copy);
  }
  
  @Test
  public void testFileID() throws Exception {
    FileID base = new FileID();
    base.setFileID(5L);
    FileID copy = new FileID();
    testAttribute(base, copy);
  }
  
  @Test
  public void testFileSystemID() throws Exception {
    FileSystemID base = new FileSystemID();
    base.setMajor(5L);
    base.setMinor(15L);
    FileSystemID copy = new FileSystemID();
    testAttribute(base, copy);
  }
  
  @Test
  public void testMode() throws Exception {
    Mode base = new Mode();
    base.setMode(15);
    Mode copy = new Mode();
    testAttribute(base, copy);
  }
  
  @Test
  public void testModifyTime() throws Exception {
    ModifyTime base = new ModifyTime();
    base.setTime(new Time(5, 15));
    ModifyTime copy = new ModifyTime();
    testAttribute(base, copy);
  }
  
  @Test
  public void testOwner() throws Exception {
    Owner base = new Owner();
    base.setOwner("brock");
    Owner copy = new Owner();
    testAttribute(base, copy);
  }
  
  @Test
  public void testOwnerGroup() throws Exception {
    OwnerGroup base = new OwnerGroup();
    base.setOwnerGroup("noland");
    OwnerGroup copy = new OwnerGroup();
    testAttribute(base, copy);
  }
  
  @Test
  public void testSize() throws Exception {
    Size base = new Size();
    base.setSize(15);
    Size copy = new Size();
    testAttribute(base, copy);
  }
  
  
  @Test
  public void testType() throws Exception {
    Type base = new Type();
    base.setType(15);
    Type copy = new Type();
    testAttribute(base, copy);
  }
  
  protected static void testAttribute(Attribute base, Attribute copy) {
    copy(base, copy);
    deepEquals(base, copy);
    RPCBuffer buffer = new RPCBuffer();
    copy.write(buffer);
    buffer.flip();
    Attribute parsed = Attribute.parse(buffer, copy.getID());
    assertEquals(base.getClass(), parsed.getClass());
  }
}
