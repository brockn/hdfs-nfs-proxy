package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.cloudera.hadoop.hdfs.nfs.Bytes;

public class FileBackedByteArray {

  private final File file;
  private final long offset;
  private final int length;
  private final int hashCode;

  private FileBackedByteArray(File file, int hashCode, long offset, int length) {
    this.file = file;
    this.hashCode = hashCode;
    this.offset = offset;
    this.length = length;
  }

  public byte[] getByteArray() throws IOException {
    RandomAccessFile randomAccessFile = null;
    try {
      randomAccessFile = new RandomAccessFile(file, "r");
      randomAccessFile.seek(offset);
      int actual = randomAccessFile.readInt();
      if(length != actual) {
        throw new IllegalStateException("File " + file + "expected length of "
            + length + " and found " + actual);
      }
      byte[] buffer = new byte[length];
      randomAccessFile.readFully(buffer);
      return buffer;
    } finally {
      if(randomAccessFile != null) {
        randomAccessFile.close();
      }
    }
  }

  public int getArrayHashCode() {
    return hashCode;
  }

  public File getFile() {
    return file;
  }

  public long getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  public static FileBackedByteArray create(File file, RandomAccessFile randomAccessFile,
      byte[] buffer, int start, int length)
      throws IOException {
    long offset = randomAccessFile.length();
    randomAccessFile.writeInt(length);
    randomAccessFile.write(buffer);
    return new FileBackedByteArray(file, Bytes.hashBytes(buffer, start, length),
        offset, length);
  }

}
