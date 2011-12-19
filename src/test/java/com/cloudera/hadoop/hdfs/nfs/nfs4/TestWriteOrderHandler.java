package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

public class TestWriteOrderHandler {

  
  @Test
  public void testSimple() throws Exception {
    FSDataOutputStream out = mock(FSDataOutputStream.class);
    final AtomicLong count = new AtomicLong(0);
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = checkNotNull(invocation.getArguments());
        checkArgument(args.length == 3);
        count.addAndGet((Integer)args[2]);
        return null;
      }      
    }).when(out).write((byte[]) any(), anyInt(), anyInt());
    when(out.getPos()).thenAnswer(new Answer<Long>() {

      @Override
      public Long answer(InvocationOnMock invocation) {
        return count.get();
      }
    });
    final WriteOrderHandler writeOrderHandler = new WriteOrderHandler(out);
    writeOrderHandler.setDaemon(true);
    writeOrderHandler.setName("WriteOrderHandler");
    writeOrderHandler.start();
    final List<Object> errors = Collections.synchronizedList(Lists.newArrayList());
    int numThreads = 50;
    final AtomicInteger xid = new AtomicInteger(0);
    final byte[] buffer = new byte[1000];
    for (int i = numThreads; i > 0; i--) {
      final long offset = (i-1) * buffer.length;
      new Thread() {
        public void run() {
          try {
            int count = writeOrderHandler.write(xid.incrementAndGet(), offset, false, buffer, 0, buffer.length);
            if(count != buffer.length) {
              errors.add(new Exception("Expected to write " + buffer.length + " but wrote " + count));
            }
          } catch (Exception e) {
            errors.add(e);
          }
        }
      }.start();
    }
    {
      long start = System.currentTimeMillis();
      long expectedPos = buffer.length * numThreads;
      while(expectedPos != out.getPos()) {
        Thread.sleep(1);
        if(!errors.isEmpty()) {
          String msg = "";
          for(Object o : errors) {
            Exception e = ((Exception)o);
            e.printStackTrace();
            msg += e.getClass().getName() + " => " + e.getMessage() + "\n";
          }
          fail(msg);
        }
        if(System.currentTimeMillis() - start > 10000) {
          fail("Timed out waiting for writes: expectedPos = " + expectedPos + ", pos = " + out.getPos());
        }
      }    
  }
    
    writeOrderHandler.close();
  }
}