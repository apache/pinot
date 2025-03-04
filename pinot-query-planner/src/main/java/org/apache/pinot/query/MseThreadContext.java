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
package org.apache.pinot.query;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.spi.executor.DecoratorExecutorService;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.LoggerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link MseThreadContext} class provides a way to set and retrieve the MSE specific information current thread.
 *
 * This class follows the same design principles as {@link QueryThreadContext} and therefore the code is more complex
 * than a reader may expect. The main reason for this complexity is to ensure that the context is properly initialized,
 * closed and kept in the current thread. This is important to avoid leaking context information or having
 * multi-threading issues. Please read the documentation of {@link QueryThreadContext} for more information.
 *
 * The {@link MseThreadContext} is used to set the stage id and worker id of the current thread. These values are used
 * to identify the stage and worker that are processing the query. The stage id is used to identify the stage of the
 * query execution and the worker id is used to identify the worker that is processing the query. In conjunction with
 * the query id, these three values can be used to uniquely identify a worker processing a query in MSE.
 *
 * It is guaranteed that all server threads running MSE operators will have this context initialized. The same can be
 * said for the server threads running SSE operators spawned by the MSE operators.
 * However, the context is not guaranteed to be initialized in the threads running SSE operators spawned by the SSE.
 */
public abstract class MseThreadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(MseThreadContext.class);
  private static final ThreadLocal<Instance> THREAD_LOCAL = new ThreadLocal<>();

  /**
   * Private constructor to prevent instantiation.
   *
   * Use {@link #open()} to initialize the context instead.
   */
  private MseThreadContext() {
  }

  private static MseThreadContext.Instance get() {
    Instance instance = THREAD_LOCAL.get();
    if (instance == null) {
      LOGGER.error("MseThreadContext is not initialized");
      throw new IllegalStateException("MseThreadContext is not initialized");
    }
    return instance;
  }

  /**
   * Returns {@code true} if the {@link MseThreadContext} is initialized in the current thread.
   *
   * Initializing the context means that the {@link #open()} method was called and the returned object is not closed
   * yet.
   */
  public static boolean isInitialized() {
    return THREAD_LOCAL.get() != null;
  }

  /**
   * Captures the state of the {@link MseThreadContext} in the current thread.
   *
   * This method is used to capture the state of the {@link MseThreadContext} in the current thread so that it can be
   * restored later in another thread.
   *
   * @return a {@link Memento} object that captures the state of the {@link MseThreadContext}
   * @throws IllegalStateException if the {@link MseThreadContext} is not initialized
   */
  public static Memento createMemento() {
    return new Memento(get());
  }

  /**
   * Initializes the {@link MseThreadContext} with default values.
   *
   * This method will throw an {@link IllegalStateException} if the {@link MseThreadContext} is already initialized.
   * That indicates an error in the code. Older context must be closed before opening a new one.
   *
   * @return an {@link AutoCloseable} object that should be used within a try-with-resources block
   * @throws IllegalStateException if the {@link MseThreadContext} is already initialized.
   */
  public static QueryThreadContext.CloseableContext open() {
    return open(null);
  }

  /**
   * Initializes the {@link MseThreadContext} with the state captured in the given {@link Memento} object, if any.
   *
   * This method will throw an {@link IllegalStateException} if the {@link MseThreadContext} is already initialized.
   * That indicates an error in the code. Older context must be closed before opening a new one.
   *
   * Values that were set in the {@link Memento} object will be set in the {@link MseThreadContext} and therefore
   * they couldn't be set again in the current thread (at least until the returned {@link AutoCloseable} is closed).
   *
   * @param memento the {@link Memento} object to capture the state from
   *                (if {@code null}, the context will be initialized with default values)
   * @return an {@link AutoCloseable} object that should be used within a try-with-resources block
   * @throws IllegalStateException if the {@link MseThreadContext} is already initialized.
   */
  public static QueryThreadContext.CloseableContext open(@Nullable Memento memento) {
    if (THREAD_LOCAL.get() != null) {
      LOGGER.error("MseThreadContext is already initialized");
      throw new IllegalStateException("MseThreadContext is already initialized");
    }

    MseThreadContext.Instance context = new MseThreadContext.Instance();
    if (memento != null) {
      context.setStageId(memento._stageId);
      context.setWorkerId(memento._workerId);
    }

    THREAD_LOCAL.set(context);

    return context;
  }

  /**
   * Returns a new {@link ExecutorService} whose tasks will be executed with the {@link MseThreadContext} initialized
   * with the state of the thread submitting the tasks.
   *
   * @param executorService the {@link ExecutorService} to decorate
   */
  public static ExecutorService contextAwareExecutorService(ExecutorService executorService) {
    return contextAwareExecutorService(executorService, MseThreadContext::createMemento);
  }

  /**
   * Returns a new {@link ExecutorService} whose tasks will be executed with the {@link MseThreadContext} initialized
   * with the state captured in the given {@link MseThreadContext.Memento} object.
   *
   * @param executorService the {@link ExecutorService} to decorate
   * @param mementoSupplier a supplier that provides the {@link Memento} object to capture the state from
   */
  public static ExecutorService contextAwareExecutorService(
      ExecutorService executorService,
      Supplier<Memento> mementoSupplier) {

    return new DecoratorExecutorService(executorService) {
      @Override
      protected <T> Callable<T> decorate(Callable<T> task) {
        MseThreadContext.Memento memento = mementoSupplier.get();
        return () -> {
          try (QueryThreadContext.CloseableContext ignored = open(memento)) {
            return task.call();
          }
        };
      }

      @Override
      protected Runnable decorate(Runnable task) {
        MseThreadContext.Memento memento = mementoSupplier.get();
        return () -> {
          try (QueryThreadContext.CloseableContext ignored = open(memento)) {
            task.run();
          }
        };
      }
    };
  }

  /**
   * Returns the stage id of the current thread.
   *
   * The default value is -1.
   * @throws IllegalStateException if the {@link MseThreadContext} is not initialized
   */
  public static int getStageId() {
    return get().getStageId();
  }

  /**
   * Sets the stage id of the current thread.
   *
   * The stage id can only be set once.
   *
   * @throws IllegalStateException if the stage id is already set or if the {@link MseThreadContext} is not initialized
   */
  public static void setStageId(int stageId) {
    get().setStageId(stageId);
  }

  /**
   * Returns the worker id of the current thread.
   *
   * The default value is -1.
   * @throws IllegalStateException if the {@link MseThreadContext} is not initialized
   */
  public static int getWorkerId() {
    return get()._workerId;
  }

  /**
   * Sets the worker id of the current thread.
   *
   * The worker id can only be set once.
   *
   * @throws IllegalStateException if the worker id is already set or if the {@link MseThreadContext} is not initialized
   */
  public static void setWorkerId(int workerId) {
    get().setWorkerId(workerId);
  }

  private static class Instance implements QueryThreadContext.CloseableContext {
    private int _stageId = -1;
    private int _workerId = -1;

    public int getStageId() {
      return _stageId;
    }

    public void setStageId(int stageId) {
      Preconditions.checkState(_stageId == -1, "Stage id already set to %s, cannot set again", _stageId);
      LoggerConstants.STAGE_ID_KEY.registerInMdc(Integer.toString(stageId));
      _stageId = stageId;
    }

    public int getWorkerId() {
      return _workerId;
    }

    public void setWorkerId(int workerId) {
      Preconditions.checkState(_workerId == -1, "Worker id already set to %s, cannot set again", _workerId);
      LoggerConstants.WORKER_ID_KEY.registerInMdc(Integer.toString(workerId));
      _workerId = workerId;
    }

    @Override
    public void close() {
      THREAD_LOCAL.remove();
      if (_stageId != -1) {
        LoggerConstants.STAGE_ID_KEY.registerInMdc(null);
      }
      if (_workerId != -1) {
        LoggerConstants.WORKER_ID_KEY.registerInMdc(null);
      }
    }
  }

  public static class Memento {
    private final int _stageId;
    private final int _workerId;

    private Memento(Instance instance) {
      _stageId = instance.getStageId();
      _workerId = instance.getWorkerId();
    }
  }
}
