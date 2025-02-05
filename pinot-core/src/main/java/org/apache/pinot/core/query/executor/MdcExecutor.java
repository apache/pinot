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
package org.apache.pinot.core.query.executor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;


/**
 * A delegator executor service that sets MDC context for the query.
 *
 * By using this executor, all tasks submitted to the executor will have the MDC context set to the query context.
 * This is easier and safer to apply than setting the MDC context manually for each task.
 *
 * TODO: Convert this class and its usages into an Executor instead of an ExecutorService
 */
public abstract class MdcExecutor implements ExecutorService {
  private final ExecutorService _executorService;

  public MdcExecutor(ExecutorService executorService) {
    _executorService = executorService;
  }

  protected abstract boolean alreadyRegistered();

  protected abstract void registerOnMDC();

  protected abstract void unregisterFromMDC();

  private <T> Callable<T> mdcCallable(Callable<T> task) {
    return () -> {
      if (alreadyRegistered()) {
        return task.call();
      }
      try {
        registerOnMDC();
        return task.call();
      } finally {
        unregisterFromMDC();
      }
    };
  }

  private <T> Collection<? extends Callable<T>> mdcCallables(Collection<? extends Callable<T>> tasks) {
    return tasks.stream().map(this::mdcCallable).collect(Collectors.toList());
  }

  private Runnable mdcRunnable(Runnable task) {
    return () -> {
      if (alreadyRegistered()) {
        task.run();
        return;
      }
      try {
        registerOnMDC();
        task.run();
      } finally {
        unregisterFromMDC();
      }
    };
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
    return _executorService.submit(mdcCallable(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return _executorService.submit(mdcRunnable(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return _executorService.submit(mdcRunnable(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return _executorService.invokeAll(mdcCallables(tasks));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
      long timeout, TimeUnit unit)
      throws InterruptedException {
    return _executorService.invokeAll(mdcCallables(tasks), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return _executorService.invokeAny(mdcCallables(tasks));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return _executorService.invokeAny(mdcCallables(tasks), timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    _executorService.execute(command);
  }
}
