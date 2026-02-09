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
package org.apache.pinot.core.data.manager;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


/**
 * An ExecutorService wrapper that automatically wraps all submitted tasks with segment operations context.
 */
public class SegmentOperationsExecutorService implements ExecutorService {
  private final ExecutorService _delegate;
  private final SegmentOperationsTaskType _taskType;
  private final String _tableNameWithType;

  public SegmentOperationsExecutorService(ExecutorService delegate, SegmentOperationsTaskType taskType,
      @Nullable String tableNameWithType) {
    _delegate = delegate;
    _taskType = taskType;
    _tableNameWithType = tableNameWithType;
  }

  @Override
  public void execute(Runnable command) {
    _delegate.execute(SegmentOperationsTaskWrapper.wrap(command, _taskType, _tableNameWithType));
  }

  @Override
  public void shutdown() {
    _delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return _delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return _delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return _delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException {
    return _delegate.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return _delegate.submit(SegmentOperationsTaskWrapper.wrap(task, _taskType, _tableNameWithType));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return _delegate.submit(SegmentOperationsTaskWrapper.wrap(task, _taskType, _tableNameWithType), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return _delegate.submit(SegmentOperationsTaskWrapper.wrap(task, _taskType, _tableNameWithType));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return _delegate.invokeAll(
        tasks.stream()
            .map(task -> SegmentOperationsTaskWrapper.wrap(task, _taskType, _tableNameWithType))
            .collect(Collectors.toList()));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return _delegate.invokeAll(
        tasks.stream()
            .map(task -> SegmentOperationsTaskWrapper.wrap(task, _taskType, _tableNameWithType))
            .collect(Collectors.toList()),
        timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return _delegate.invokeAny(
        tasks.stream()
            .map(task -> SegmentOperationsTaskWrapper.wrap(task, _taskType, _tableNameWithType))
            .collect(Collectors.toList()));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return _delegate.invokeAny(
        tasks.stream()
            .map(task -> SegmentOperationsTaskWrapper.wrap(task, _taskType, _tableNameWithType))
            .collect(Collectors.toList()),
        timeout, unit);
  }
}
