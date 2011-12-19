package com.cloudera.hadoop.hdfs.nfs;

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCPacket;
import com.cloudera.hadoop.hdfs.nfs.security.AuthenticatedCredentials;
import com.cloudera.hadoop.hdfs.nfs.security.CredentialsUnix;

public class TestUtils {
  protected static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  public static AuthenticatedCredentials newCredentials() {
    CredentialsUnix creds = new CredentialsUnix();
    creds.setUID(0);
    creds.setGID(0);
    creds.setAuxGIDs(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    return creds;
  }
  public static void copy(MessageBase base, MessageBase copy) {
    RPCBuffer buffer = new RPCBuffer();
    base.write(buffer);
    buffer.flip();
    if(base instanceof RPCPacket) {
      buffer.skip(4); // size
    }
    copy.read(buffer);
  }

  public static void deepEquals(MessageBase base, MessageBase copy) {
    AtomicInteger count = new AtomicInteger(0);
    deepEquals(base, copy, count);
    if(count.get() <= 0) {
      LOGGER.error("Did not test any methods for " + base.getClass().getName());
    }
  }
  protected static void deepEquals(MessageBase base, MessageBase copy, AtomicInteger count) {
    assertEquals(base.getClass(), copy.getClass());
    String prefix = null;
    try {
      Class<?> clazz = base.getClass(); 
      for (Method method : clazz.getMethods()) {
        prefix = clazz.getName() + ":" + method.getName();
        int mod = method.getModifiers();
        if(method.getName().startsWith("get") && method.getParameterTypes().length == 0 &&
            !(Modifier.isStatic(mod) || Modifier.isAbstract(mod) || Modifier.isNative(mod))) {
          Object baseResult = method.invoke(base, (Object[])null);
          Object copyResult = method.invoke(base, (Object[])null);
          if (baseResult instanceof MessageBase) {
            deepEquals((MessageBase)baseResult, (MessageBase)copyResult, count);
          } else if (baseResult instanceof List) {
            List<?> baseList = (List<?>)baseResult;
            List<?> copyList = (List<?>)copyResult;
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
