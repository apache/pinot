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
 * Specifically, all tasks submitted to the ExecutorService are decorated before they are executed.
 * This allows to add functionality before and after the task is executed without modifying the task itself.
 *
 * For example, {@link MdcExecutor} uses this to set the MDC context before the task is executed and clear it after the
 * execution without having to modify the task itself. Implementations may also use {@link BeforeAfter} instead of this
 * class, given it already includes methods to be executed before and after the task without having to deal with the
 * decoration process.
 *
 * TODO: Convert this class and its usages into an Executor instead of an ExecutorService
 */
public abstract class DecoratorExecutorService implements ExecutorService {
  private final ExecutorService _executorService;

  public DecoratorExecutorService(ExecutorService executorService) {
    _executorService = executorService;
  }

  /**
   * Decorates the callable task.
   *
   * This method is called by the submit, invokeAll, and invokeAny methods to decorate the task before it is executed.
   *
   * Usually implementations should return a new Callable that wraps the received task. The new Callable should call the
   * received task in its call method, and do whatever is needed before and after the call.
   *
   * For example {@link MdcExecutor} uses this to set the MDC context before the task is executed and clear it after the
   * execution without having to modify the task itself.
   *
   * There are three important places to decorate or intercept the task:
   *
   * <ol>
   *   <li>At the beginning of the task execution. This is done by executing code inside the decorator Callable before
   *   calling the task.call() method.</li>
   *   <li>At the end of the task execution. This is done by executing code inside the decorator Callable after calling
   *   the task.call() method.</li>
   *   <li>Before the task is submitted to the executor. This is done by executing code directly in this method.
   *   For example, one implementation could use that to count how many tasks have been submitted to the executor<./li>
   * </ol>
   *
   * @param task the actual task that has to be executed
   * @return the decorated task
   */
  protected abstract <T> Callable<T> decorate(Callable<T> task);

  /**
   * Like {@link #decorate(Callable)} but for Runnable tasks.
   */
  protected abstract Runnable decorate(Runnable task);

  /**
   *
   * @param tasks
   * @return
   * @param <T>
   */
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
     * Called after the runnable/callable is executed, only if it was successful
     */
    public abstract void afterSuccess();

    /**
     * Called after the runnable/callable is executed, even if it fails.
     * This is the equivalent to a finally block in a try/catch (but cannot be called finally because it is a reserved
     * keyword in Java).
     */
    public abstract void afterAnything();

    @Override
    protected <T> Callable<T> decorate(Callable<T> task) {
      return () -> {
        try {
          before();
          T result = task.call();
          afterSuccess();
          return result;
        } finally {
          afterAnything();
        }
      };
    }

    @Override
    protected Runnable decorate(Runnable task) {
      return () -> {
        try {
          before();
          task.run();
          afterSuccess();
        } finally {
          afterAnything();
        }
      };
    }
  }

  public static abstract class WithAutoCloseable extends DecoratorExecutorService {
    public WithAutoCloseable(ExecutorService executorService) {
      super(executorService);
    }

    protected abstract AutoCloseable open();

    @Override
    protected <T> Callable<T> decorate(Callable<T> task) {
      return () -> {
        try (AutoCloseable closeable = open()) {
          return task.call();
        }
      };
    }

    @Override
    protected Runnable decorate(Runnable task) {
      return () -> {
        try (AutoCloseable closeable = open()) {
          task.run();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };
    }
  }
}
