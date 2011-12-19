package com.cloudera.hadoop.hdfs.nfs.tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class BasicTest {
  public void runCreateThenRead(String base, final int length, final int randomReads) throws Exception {
    File nfsRoot = new File(base);
    nfsRoot.mkdirs();
    byte[] randomReadBuffer = new byte[1024];
    File file = new File(nfsRoot, UUID.randomUUID().toString());

    {
      int size = length;
      FileOutputStream out = new FileOutputStream(file);
      while(size-- > 0) {
        out.write((byte)'A');
      }
      out.close();
      System.out.println(new Date() +" Write Test passed");
    }

    /*
     * Do a number of random reads
     */
    {
      RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
      Random random = new Random();
      for (int k = 0; k < randomReads; k++) {
        int offset = random.nextInt(length - 10);
        randomAccessFile.seek(offset);
        int numOfBytes = random.nextInt(Math.min(randomReadBuffer.length, length - offset));
        int count;
        if((count = randomAccessFile.read(randomReadBuffer, 0, numOfBytes)) < 0) {
          throw new RuntimeException("Hit EOF offset, " + offset + ", numOfBytes " + numOfBytes);
        }
        for (int i = 0; i < count; i++) {
          if((char)randomReadBuffer[i] != 'A') {
            throw new RuntimeException("Bad data '" + Integer.toHexString(randomReadBuffer[i]) + "' @ " + count);
          }
        }
      }
      randomAccessFile.close();
      System.out.println(new Date() +" Random Read Test passed");
    }

    {
      try {          
        @SuppressWarnings("unused")
        FileOutputStream out = new FileOutputStream(file, true);
        throw new RuntimeException("was able to open file in append mode");
      } catch(FileNotFoundException x) {
        // expected
      }
      System.out.println(new Date() +" Append Test passed");
    }

    
    /*
     * Read file sequentially, verifying the contents
     */
    {
      InputStream is = new FileInputStream(file);
      int i = is.read();
      int count = 0;
      while(i >= 0) {
        count++;
        if((char)i != 'A') {
          throw new RuntimeException("Bad data '" + Integer.toHexString(i) + "' @ " + count);
        }
        i = is.read();
      }
      if(count != length) {
        throw new RuntimeException("Bad length '" + count + "'");
      }
      is.close();
      System.out.println(new Date() +" Sequential Read Test passed");
    }
    
    /*
     * Try and open file in rw mode
     */
    {
      try {
        @SuppressWarnings("unused")
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        throw new RuntimeException("was able to open " + file + " in rw mode");
      } catch (FileNotFoundException e) {
        // expected
      }
      System.out.println(new Date() +" Random Write Test passed");
    }
    
    if(!file.delete()) {
      throw new Exception("delete " + file + " failed");
    }
    System.out.println(new Date() +" Delete Test Passed");
  }
    
  public static void main(String[] args) throws Exception {
    BasicTest test = new BasicTest();
    String name = args[0];
    try {
      test.runCreateThenRead(name, 1024 * 1024, 100);
    } catch(Exception ex) {
      // we expect md5sum e6065c4aa2ab1603008fc18410f579d4
      System.err.println("Error on file " + name);
      ex.printStackTrace();
      System.exit(-1);
    }
  } 
}