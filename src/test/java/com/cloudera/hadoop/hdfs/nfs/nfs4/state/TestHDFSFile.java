package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.UUID;

import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData12;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.google.common.base.Charsets;

public class TestHDFSFile {

  private HDFSFile hdfsFile;
  private FileHandle fileHandle;
  private String path;
  private long fileID;
  
  private StateID stateID;
  
  private HDFSOutputStream out;
  private FSDataInputStream in;
  
  @Before
  public void setup() throws Exception {
    fileHandle = new FileHandle(UUID.randomUUID().toString().getBytes(Charsets.UTF_8));
    path = "/file";
    fileID = 1;
    hdfsFile = new HDFSFile(fileHandle, path, fileID);
    
    stateID = new StateID();
    OpaqueData12 opaque = new OpaqueData12();
    opaque.setData("1".getBytes(Charsets.UTF_8));
    stateID.setData(opaque);
    
    out = mock(HDFSOutputStream.class);
    in = mock(FSDataInputStream.class);
  }
  
  @Test
  public void testGetters() throws Exception {
    Assert.assertSame(fileHandle, hdfsFile.getFileHandle());
    Assert.assertSame(path, hdfsFile.getPath());
    Assert.assertSame(fileID, hdfsFile.getFileID());
    Assert.assertNull(hdfsFile.getHDFSOutputStream());
    Assert.assertNull(hdfsFile.getHDFSOutputStreamForWrite());
    Assert.assertNull(hdfsFile.getFSDataInputStream(new StateID()));
    hdfsFile.toString();
  }
  @Test
  public void testIsOpen() throws Exception {
    Assert.assertFalse(hdfsFile.isOpen());
    Assert.assertFalse(hdfsFile.isOpenForRead());
    Assert.assertFalse(hdfsFile.isOpenForWrite());
  }
  
  @Test
  public void testOutputStream() throws Exception {
    hdfsFile.setHDFSOutputStream(stateID, out);
    Assert.assertSame(out, hdfsFile.getHDFSOutputStream().get());
    Assert.assertSame(out, hdfsFile.getHDFSOutputStreamForWrite().get());
    Assert.assertTrue(hdfsFile.isOpen());
    Assert.assertTrue(hdfsFile.isOpenForWrite());
    Assert.assertFalse(hdfsFile.isOpenForRead());
    hdfsFile.closeOutputStream(stateID);
    verify(out).close();
  }
  
  @Test
  public void testInputStream() throws Exception {
    hdfsFile.putFSDataInputStream(stateID, in);
    Assert.assertSame(in, hdfsFile.getFSDataInputStream(stateID).get());
    Assert.assertTrue(hdfsFile.isOpen());
    Assert.assertTrue(hdfsFile.isOpenForRead());
    Assert.assertFalse(hdfsFile.isOpenForWrite());
    hdfsFile.closeInputStream(stateID);
    verify(in).close();
  }
  
  @Test
  public void testTimestamp() throws Exception {
    hdfsFile.putFSDataInputStream(stateID, in);
    hdfsFile.setHDFSOutputStream(stateID, out);
    long inputLastUsedBefore = hdfsFile.getFSDataInputStream(stateID).getTimestamp();
    long outputLastUsedBefore = hdfsFile.getHDFSOutputStreamForWrite().getTimestamp();
    Thread.sleep(1001L);
    long inputLastUsedAfter = hdfsFile.getFSDataInputStream(stateID).getTimestamp();
    long outputLastUsedAfter = hdfsFile.getHDFSOutputStreamForWrite().getTimestamp();
    Assert.assertTrue(inputLastUsedAfter - inputLastUsedBefore >= 1000);
    Assert.assertTrue(outputLastUsedAfter - outputLastUsedBefore >= 1000);
  }
}
