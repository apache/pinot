/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;


/**
 * ScalingThreadPoolExecutor is an auto-scaling ThreadPoolExecutor. If there is no available thread for a new task,
 * a new thread will be created by the internal ThreadPoolExecutor to process the task (up to maximumPoolSize). If
 * there is an available thread, no additional thread will be created.
 *
 * This is done by creating a ScalingQueue that will 'reject' a new task if there are no available threads, forcing
 * the pool to create a new thread. The rejection is then handled to queue the task anyway.
 *
 * This differs from the plain ThreadPoolExecutor implementation which does not create new threads if the queue (not
 * thread pool) has capacity. For a more complete explanation, see:
 * https://github.com/kimchy/kimchy.github.com/blob/master/_posts/2008-11-23-juc-executorservice-gotcha.textile
 */
public class ScalingThreadPoolExecutor extends ThreadPoolExecutor {
  private final AtomicInteger _activeCount = new AtomicInteger();

  public ScalingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  @Override
  public int getActiveCount() {
    return _activeCount.get();
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    _activeCount.incrementAndGet();
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    _activeCount.decrementAndGet();
  }

  /**
   * Creates a new ScalingThreadPoolExecutor
   *
   * @param min minimum pool size
   * @param max maximum pool size
   * @param keepAliveTime idle thread keepAliveTime (milliseconds)
   * @return auto-scaling ExecutorService
   */
  public static ExecutorService newScalingThreadPool(int min, int max, long keepAliveTime) {
    ScalingQueue<Runnable> queue = new ScalingQueue<>();
    ThreadPoolExecutor executor = new ScalingThreadPoolExecutor(min, max, keepAliveTime, TimeUnit.MILLISECONDS, queue);
    executor.setRejectedExecutionHandler(new ForceQueuePolicy());
    queue.setThreadPoolExecutor(executor);
    return executor;
  }

  /**
   * Used to handle queue rejections. The policy ensures we still queue the Runnable, and the rejection ensures the pool
   * will be expanded if necessary
   */
  static class ForceQueuePolicy implements RejectedExecutionHandler {
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      try {
        executor.getQueue().put(r);
      } catch (InterruptedException e) {
        // should never happen since we never wait
        throw new RejectedExecutionException(e);
      }
    }
  }

  /**
   * Used to reject new elements if there is no available thread. Rejections will be handled by {@link ForceQueuePolicy}
   */
  static class ScalingQueue<E> extends LinkedBlockingQueue<E> {

    private ThreadPoolExecutor _executor;

    // Creates a queue of size Integer.MAX_SIZE
    public ScalingQueue() {
      super();
    }

    // Sets the executor this queue belongs to
    public void setThreadPoolExecutor(ThreadPoolExecutor executor) {
      _executor = executor;
    }

    /**
     * Inserts the specified element at the tail of this queue if there is at least one available thread
     * to run the current task. If all pool threads are actively busy, it rejects the offer.
     *
     * @param e the element to add.
     * @return true if it was possible to add the element to this queue, else false
     */
    @Override
    public boolean offer(@Nonnull E e) {
      int allWorkingThreads = _executor.getActiveCount() + super.size();
      return allWorkingThreads < _executor.getPoolSize() && super.offer(e);
    }
  }
}
