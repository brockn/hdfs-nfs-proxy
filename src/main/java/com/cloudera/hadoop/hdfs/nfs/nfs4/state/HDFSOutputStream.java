package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;

public class HDFSOutputStream extends OutputStream {

  private final FSDataOutputStream outputStream;
  private final String filename;
  private long position;
  public HDFSOutputStream(FSDataOutputStream outputStream, String filename) {
    super();
    this.outputStream = outputStream;
    this.filename = filename;
    this.position = 0L;
  }
  public void write(byte b[]) throws IOException {
    write(b, 0, b.length);
  }
  public void write(byte b[], int off, int len) throws IOException {
    outputStream.write(b, off, len);
    position += len;
  }
  @Override
  public void write(int b) throws IOException {
    outputStream.write(b);
    position++;
  }
  public long getPos()  {
    return position;
  }
  public void sync() throws IOException {
    outputStream.sync();
  }
  @Override
  public String toString() {
    return "HDFSOutputStream [outputStream=" + outputStream + ", filename="
        + filename + ", position=" + position + "]";
  }
}
