package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.util.concurrent.BlockingQueue;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.util.concurrent.AbstractFuture;
import static org.fest.reflect.core.Reflection.*;
public class TestAsyncTaskExecutor {

  
  @Test
  public void testRetry() throws InterruptedException {
    AsyncTaskExecutor<Void> executor = new AsyncTaskExecutor<Void>();
    
    AsyncFutureImpl task1 = new AsyncFutureImpl();
    executor.schedule(task1);
    BlockingQueue queue = field("queue")
        .ofType(BlockingQueue.class)
        .in(executor)
        .get();    
    Thread.sleep(2000L);
    Assert.assertTrue(queue.isEmpty());
  }
  
  private static class AsyncFutureImpl extends AbstractFuture<Void> 
  implements AsyncFuture<Void> {  
    boolean called;
    @Override
    public AsyncFuture.Complete makeProgress() {
      if(called) {
        return AsyncFuture.Complete.COMPLETE;
      }
      called = true;
      return AsyncFuture.Complete.RETRY;
    }    
  }
} 
