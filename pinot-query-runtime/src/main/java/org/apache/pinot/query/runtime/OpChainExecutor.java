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
package org.apache.pinot.query.runtime;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OpChainExecutor implements ExecutorService, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpChainExecutor.class);
  private final ExecutorService _executorService;

  public OpChainExecutor(ExecutorService executorService) {
    _executorService = executorService;
  }

  public OpChainExecutor(ThreadFactory threadFactory) {
    this(Executors.newCachedThreadPool(threadFactory));
  }

  @Override
  public void shutdown() {
    _executorService.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return _executorService.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return _executorService.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return _executorService.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException {
    return _executorService.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return _executorService.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return _executorService.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return _executorService.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return _executorService.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return _executorService.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return _executorService.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return _executorService.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    _executorService.execute(command);
  }

  @Override
  public void close() throws RuntimeException {
    _executorService.shutdown();
    try {
      if (!_executorService.awaitTermination(30, TimeUnit.SECONDS)) {
        List<Runnable> runnables = _executorService.shutdownNow();
        LOGGER.warn("Around " + runnables.size() + " didn't finish in time after a shutdown");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
