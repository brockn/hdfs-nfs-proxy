/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.hadoop.hdfs.nfs.tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class StressTest {
  public void runCreateThenRead(int threads, final int files, final int length, final int randomReads) throws Exception {
    final File nfsRoot = new File("/mnt/hdfs/tmp/nfs-test");
    nfsRoot.mkdirs();
    final AtomicInteger threadsAlive = new AtomicInteger(threads); 
    for (int i = 0; i < threads; i++) {
      new Thread() {
        public void run() {
          try {
            byte[] randomReadBuffer = new byte[1024];
            for (int j = 0; j < files; j++) {
              int size = length;
              File file = new File(nfsRoot, UUID.randomUUID().toString());
              FileOutputStream out = new FileOutputStream(file);
              while(size-- > 0) {
                out.write((byte)'A');
              }
              out.close();

              /*
               * Read file sequentially, verifying the contents
               */
              long start = System.currentTimeMillis();
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
              System.out.println("Sequential read took " + (System.currentTimeMillis() - start) + "ms");
              /*
               * Do a number of random reads
               */
              RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
              Random random = new Random();
              start = System.currentTimeMillis();
              for (int k = 0; k < randomReads; k++) {
                int offset = random.nextInt(length - 10);
                randomAccessFile.seek(offset);
                int numOfBytes = random.nextInt(Math.min(randomReadBuffer.length, length - offset));
                if(randomAccessFile.read(randomReadBuffer, 0, numOfBytes) < 0) {
                  throw new RuntimeException("Hit EOF offset, " + offset + ", numOfBytes " + numOfBytes);
                }
              }
              System.out.println("Random reads (" + randomReads + ") took " + (System.currentTimeMillis() - start) + "ms");
              randomAccessFile.close();
              file.delete();
            }
          } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
          } finally {
            threadsAlive.decrementAndGet();
          }
        }
      }.start();
    }    
    while(threadsAlive.get() > 0) {
      Thread.sleep(10L);
    }
  }
    
  public static void main(String[] args) throws Exception {
    StressTest test = new StressTest();
    while(true) {
      test.runCreateThenRead(2, 10, 1024 * 1024, 100);
    }
  }

  
}
