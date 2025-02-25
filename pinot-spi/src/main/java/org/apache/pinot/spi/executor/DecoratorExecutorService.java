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
package org.apache.pinot.spi.executor;

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
 * DecoratorExecutorService is an abstract class that provides a way to decorate an ExecutorService with additional
 * functionality.
 *
 * TODO: Convert this class and its usages into an Executor instead of an ExecutorService
 */
public abstract class DecoratorExecutorService implements ExecutorService {
  private final ExecutorService _executorService;

  public DecoratorExecutorService(ExecutorService executorService) {
    _executorService = executorService;
  }

  protected abstract <T> Callable<T> decorate(Callable<T> task);

  protected abstract Runnable decorate(Runnable task);

  protected <T> Collection<? extends Callable<T>> decorateTasks(Collection<? extends Callable<T>> tasks) {
    return tasks.stream().map(this::decorate).collect(Collectors.toList());
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
    return _executorService.submit(decorate(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return _executorService.submit(decorate(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return _executorService.submit(decorate(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return _executorService.invokeAll(decorateTasks(tasks));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
      long timeout, TimeUnit unit)
      throws InterruptedException {
    return _executorService.invokeAll(decorateTasks(tasks), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return _executorService.invokeAny(decorateTasks(tasks));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return _executorService.invokeAny(decorateTasks(tasks), timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    _executorService.execute(decorate(command));
  }

  public static abstract class BeforeAfter extends DecoratorExecutorService {
    public BeforeAfter(ExecutorService executorService) {
      super(executorService);
    }

    /**
     * Called before the runnable/callable is executed
     */
    public abstract void before();

    /**
     * Called after the runnable/callable is executed, even if it fails
     */
    public abstract void after();

    @Override
    protected <T> Callable<T> decorate(Callable<T> task) {
      return () -> {
        before();
        try {
          return task.call();
        } finally {
          after();
        }
      };
    }

    @Override
    protected Runnable decorate(Runnable task) {
      return () -> {
        before();
        try {
          task.run();
        } finally {
          after();
        }
      };
    }
  }
}
