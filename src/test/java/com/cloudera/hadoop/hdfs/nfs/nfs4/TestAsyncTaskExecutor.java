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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static org.fest.reflect.core.Reflection.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.util.concurrent.AbstractFuture;
public class TestAsyncTaskExecutor {

  
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
  
  private static class AsyncFutureImpl extends AbstractFuture<Void> 
  implements AsyncFuture<Void> {
    AtomicInteger calls = new AtomicInteger(0);
    @Override
    public AsyncFuture.Complete makeProgress() {
      if(calls.incrementAndGet() > 1) {
        return AsyncFuture.Complete.COMPLETE;
      }
      return AsyncFuture.Complete.RETRY;
    }    
  }
} 
