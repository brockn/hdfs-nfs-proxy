/**
 * Copyright 2012 Cloudera Inc.
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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static org.fest.reflect.core.Reflection.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.AsyncTaskExecutor.DelayedRunnable;
import com.google.common.util.concurrent.AbstractFuture;
public class TestAsyncTaskExecutor {


  private static class AsyncFutureImpl extends BaseAsyncFuture {
    AtomicInteger calls = new AtomicInteger(0);
    @Override
    public AsyncFuture.Complete makeProgress() {
      if(calls.incrementAndGet() > 1) {
        return AsyncFuture.Complete.COMPLETE;
      }
      return AsyncFuture.Complete.RETRY;
    }
  }
  private static abstract class BaseAsyncFuture extends AbstractFuture<Void>
  implements AsyncFuture<Void> {

  }

  @Test
  public void testEqualsHashCode() throws InterruptedException {
    DelayedRunnable delayRunnable1 = new DelayedRunnable(new Runnable() {
      @Override
      public void run() {
      }
    }, 2000L);
    Thread.sleep(5L);
    DelayedRunnable delayRunnable2 = new DelayedRunnable(new Runnable() {
      @Override
      public void run() {
      }
    }, 2000L);
    Assert.assertFalse(delayRunnable1.equals(null));
    Assert.assertFalse(delayRunnable1.equals(delayRunnable2));
    Assert.assertTrue(delayRunnable1.equals(delayRunnable1));
    Assert.assertFalse(delayRunnable1.hashCode() == delayRunnable2.hashCode());
  }

  @Test
  public void testError() throws InterruptedException {
    AsyncTaskExecutor<Void> executor = new AsyncTaskExecutor<Void>();
    BaseAsyncFuture task1 = new BaseAsyncFuture() {
      @Override
      public AsyncFuture.Complete makeProgress() {
        throw new Error();
      }
    };
    executor.schedule(task1);
    @SuppressWarnings("rawtypes")
    BlockingQueue queue = field("queue")
        .ofType(BlockingQueue.class)
        .in(executor)
        .get();
    Thread.sleep(2000L);
    Assert.assertTrue(queue.isEmpty());
  }
  @Test
  public void testException() throws InterruptedException {
    AsyncTaskExecutor<Void> executor = new AsyncTaskExecutor<Void>();
    BaseAsyncFuture task1 = new BaseAsyncFuture() {
      @Override
      public AsyncFuture.Complete makeProgress() {
        throw new RuntimeException();
      }
    };
    executor.schedule(task1);
    @SuppressWarnings("rawtypes")
    BlockingQueue queue = field("queue")
        .ofType(BlockingQueue.class)
        .in(executor)
        .get();
    Thread.sleep(2000L);
    Assert.assertTrue(queue.isEmpty());
  }
  @Test
  public void testGetDelayDelay() throws InterruptedException {
    DelayedRunnable delayRunnable = new DelayedRunnable(new Runnable() {
      @Override
      public void run() {
      }
    }, 2000L);
    Assert.assertTrue(delayRunnable.getDelay(TimeUnit.MILLISECONDS) > 1000L);
    Thread.sleep(2001L);
    Assert.assertEquals(0, delayRunnable.getDelay(TimeUnit.MILLISECONDS));
  }
  @Test
  public void testGetDelayNoDelay() throws InterruptedException {
    DelayedRunnable delayRunnable = new DelayedRunnable(new Runnable() {
      @Override
      public void run() {
      }
    });
    Assert.assertEquals(0, delayRunnable.getDelay(TimeUnit.MILLISECONDS));
  }
  @Test
  public void testRetry() throws InterruptedException {
    AsyncTaskExecutor<Void> executor = new AsyncTaskExecutor<Void>();
    AsyncFutureImpl task1 = new AsyncFutureImpl();
    executor.schedule(task1);
    @SuppressWarnings("rawtypes")
    BlockingQueue queue = field("queue")
        .ofType(BlockingQueue.class)
        .in(executor)
        .get();
    Thread.sleep(2000L);
    Assert.assertEquals(2, task1.calls.get());
    Assert.assertTrue(queue.isEmpty());
  }
  @Test
  public void testSortOrder() throws InterruptedException {
    DelayedRunnable delayRunnable1 = new DelayedRunnable(new Runnable() {
      @Override
      public void run() {
      }
    }, 2000L);
    Thread.sleep(5L);
    DelayedRunnable delayRunnable2 = new DelayedRunnable(new Runnable() {
      @Override
      public void run() {
      }
    }, 2000L);
    Assert.assertTrue(delayRunnable1.compareTo(delayRunnable2) < 0);
    Assert.assertTrue(delayRunnable2.compareTo(delayRunnable1) > 0);
    // since this is based on time, we have to retry this a few times
    // in case the time changed during the call
    boolean result = false;
    for (int i = 0; (i < 10) && !result; i++) {
      result = delayRunnable1.compareTo(delayRunnable1) == 0;
    }
    Assert.assertTrue(result);
  }
}
