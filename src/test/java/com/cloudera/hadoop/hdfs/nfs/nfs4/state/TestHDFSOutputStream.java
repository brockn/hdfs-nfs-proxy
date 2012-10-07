package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import junit.framework.Assert;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.google.common.base.Charsets;
public class TestHDFSOutputStream {

  private HDFSOutputStream out;
  private FSDataOutputStream outputStream;
  private String filename;
  private FileHandle fileHandle;
  
  @Before
  public void setup() throws Exception {
    outputStream = mock(FSDataOutputStream.class);
    fileHandle = new FileHandle("fh".getBytes(Charsets.UTF_8));
    filename = "file";
    out = new HDFSOutputStream(outputStream, filename, fileHandle);
  }
  
  @Test
  public void testWrite1() throws Exception {
    Assert.assertEquals(0L, out.getLastOperation());
    byte[] buffer = new byte[436];
    out.write(buffer, 0, buffer.length);
    verify(outputStream, times(1)).write(buffer, 0, buffer.length);
    Assert.assertTrue(System.currentTimeMillis() - out.getLastOperation() <= 1000L);
  }
  @Test
  public void testWrite2() throws Exception {
    Assert.assertEquals(0L, out.getLastOperation());
    byte[] buffer = new byte[436];
    out.write(buffer);
    verify(outputStream, times(1)).write(buffer, 0, buffer.length);
    Assert.assertTrue(System.currentTimeMillis() - out.getLastOperation() <= 1000L);
  }
  @Test
  public void testWrite3() throws Exception {
    Assert.assertEquals(0L, out.getLastOperation());
    int x = 1;
    out.write(x);
    verify(outputStream, times(1)).write(x);
    Assert.assertTrue(System.currentTimeMillis() - out.getLastOperation() <= 1000L);
  }
  
  @Test
  public void testGetPos() throws Exception {
    out.write(1);
    byte[] buffer = new byte[436];
    out.write(buffer);
    out.write(buffer, 0, buffer.length);
    Assert.assertEquals(buffer.length * 2 + 1, out.getPos());
  }
  @Test
  public void testClose() throws Exception {
    out.close();
    verify(outputStream).close();    
  }
  @Test
  public void testSync() throws Exception {
    Assert.assertEquals(0L, out.getLastOperation());
    out.sync();
    verify(outputStream).sync();    
    Assert.assertTrue(System.currentTimeMillis() - out.getLastOperation() <= 1000L);
  }
  @Test
  public void testMisc() throws Exception {
    Assert.assertSame(fileHandle, out.getFileHandle());
    out.toString();
  }
}
