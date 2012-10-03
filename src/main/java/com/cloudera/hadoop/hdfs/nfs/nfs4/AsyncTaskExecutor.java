package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class AsyncTaskExecutor<T> {
  protected static final Logger LOGGER = Logger.getLogger(AsyncTaskExecutor.class);

  private ScheduledThreadPoolExecutor executor; 
  public AsyncTaskExecutor(ScheduledThreadPoolExecutor executor) {
    this.executor = executor;
  }
  
  public void schedule(final AsyncFuture<T> task) {
    executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          if(task.makeProgress() == AsyncFuture.Complete.COMPLETE) {
            executor.getQueue().remove(task);
          }
        } catch (Exception e) {
          LOGGER.error("Unabled exception", e);
        } catch (Error e) {
          LOGGER.error("Unabled error", e);
        }
      }
    }, 0L, 1, TimeUnit.SECONDS);
  }
}
