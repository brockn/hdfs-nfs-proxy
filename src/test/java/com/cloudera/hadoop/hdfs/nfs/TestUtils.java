/**
 * Copyright 2012 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cloudera.hadoop.hdfs.nfs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;
import static org.junit.Assert.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FixedUserIDMapper;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCPacket;
import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsSystem;

public class TestUtils {

  protected static final Logger LOGGER = Logger.getLogger(TestUtils.class);
  final static public String tmpDirPath;
  final static public String tmpDirPathForTest;

  static {
    tmpDirPath = System.getProperty("java.io.tmpdir");
    tmpDirPathForTest = tmpDirPath + File.separator
        + "TestDir" + (new Date()).getTime();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          PathUtils.fullyDelete(new File(tmpDirPathForTest));
        } catch (IOException e) {

        }
      }
    });
  }

  public static Configuration setupConf() throws IOException {
    return setupConf(new Configuration());
  }

  public static String buildTempDirDataFiles(
      final int numberOfFiles,
      final String path) throws IOException {
    String retVal = tmpDirPathForTest;
    File dir = new File(retVal);
    boolean mkdirsSuccess = dir.mkdirs();
    if (!mkdirsSuccess) {
      File test = new File(retVal);
      if (!test.exists()) {
        throw new IOException("failed to make dir -> " + dir.getAbsolutePath());
      }
    }
    for (int i = 0; i < numberOfFiles; i++) {
      createTempFile(path, true, 2048, TestUtils.class.getClass().getName(), Integer.valueOf(i).toString());
    }
    return (retVal);
  }

  public static Configuration setupConf(Configuration conf) throws IOException {
    if(conf.get(USER_ID_MAPPER_CLASS) == null) {
      conf.set(USER_ID_MAPPER_CLASS, FixedUserIDMapper.class.getName());
    }
    final String dir = "/tmp/" + UUID.randomUUID().toString();
    conf.set(NFS_FILEHANDLE_STORE_FILE, dir);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          PathUtils.fullyDelete(new File(dir));
        } catch (IOException e) {

        }
      }
    });
    return conf;
  }

  public static AuthenticatedCredentials newCredentials() {
    CredentialsSystem creds = new CredentialsSystem();
    creds.setUID(0);
    creds.setGID(0);
    creds.setAuxGIDs(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    return creds;
  }

  public static void copy(MessageBase base, MessageBase copy) {
    RPCBuffer buffer = new RPCBuffer();
    base.write(buffer);
    buffer.flip();
    if (base instanceof RPCPacket) {
      buffer.skip(4); // size
    }
    copy.read(buffer);
  }

  public static void deepEquals(MessageBase base, MessageBase copy) {
    AtomicInteger count = new AtomicInteger(0);
    deepEquals(base, copy, count);
    if (count.get() <= 0) {
      LOGGER.error("Did not test any methods for " + base.getClass().getName());
    }
  }

  /**
   * create a temp file with a random set of ascii characters
   *
   * @author hanasaki
   * @param size
   * @param fnRequested
   * @return
   * @throws IOException
   */
  public static File createTempFile(
      final String path,
      final boolean deleteOnExit,
      final int size,
      final String fnRequested,
      final String fnSuffix)
          throws IOException {
    File retVal;
    //
    String fn = (fnRequested == null) ? "testFile" : fnRequested;
    fn += (fnSuffix == null) ? "" : "-" + fnSuffix;

    if (path == null) {
      retVal = File.createTempFile(fn, ".tmp");
    } else {
      File dir = new File(path);
      retVal = File.createTempFile(fn, ".tmp", dir);
    }
    if (deleteOnExit) {
      retVal.deleteOnExit();
    }

    OutputStream o = new BufferedOutputStream(new FileOutputStream(retVal));

    //
    Random random = new Random();
    final short start = ' ';
    final short end = '}';
    final short cnt = end - start;
    //
    for (int i = 0; i < size; i++) {
      short r = (short) random.nextInt(cnt);
      byte value = (byte) (start + r);
      o.write(value);
    }
    o.flush();
    o.close();

    LOGGER.debug("created " + size + " byte tmp file => " + retVal.getCanonicalPath());

    return (retVal);
  }

  protected static void deepEquals(MessageBase base, MessageBase copy, AtomicInteger count) {
    assertEquals(base.getClass(), copy.getClass());
    String prefix = null;
    try {
      Class<?> clazz = base.getClass();
      for (Method method : clazz.getMethods()) {
        prefix = clazz.getName() + ":" + method.getName();
        int mod = method.getModifiers();
        if (method.getName().startsWith("get") && method.getParameterTypes().length == 0
            && !(Modifier.isStatic(mod) || Modifier.isAbstract(mod) || Modifier.isNative(mod))) {
          Object baseResult = method.invoke(base, (Object[]) null);
          Object copyResult = method.invoke(base, (Object[]) null);
          if (baseResult instanceof MessageBase) {
            deepEquals((MessageBase) baseResult, (MessageBase) copyResult, count);
          } else if (baseResult instanceof List) {
            List<?> baseList = (List<?>) baseResult;
            List<?> copyList = (List<?>) copyResult;
            assertEquals(baseList.size(), copyList.size());
            for (int i = 0; i < baseList.size(); i++) {
              count.addAndGet(1);
              assertEquals(baseList.get(i), copyList.get(i));
            }
          } else {
            count.addAndGet(1);
            assertEquals(prefix, baseResult, copyResult);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(prefix, e);
    }
  }
}
