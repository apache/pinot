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
 * The {@link MseWorkerThreadContext} class provides a way to set and retrieve the MSE specific information current
 * thread.
 *
 * This class follows the same design principles as {@link QueryThreadContext} and therefore the code is more complex
 * than a reader may expect. The main reason for this complexity is to ensure that the context is properly initialized,
 * closed and kept in the current thread. This is important to avoid leaking context information or having
 * multi-threading issues. Please read the documentation of {@link QueryThreadContext} for more information.
 *
 * The {@link MseWorkerThreadContext} is used to set the stage id and worker id of the current thread. These values are
 * used to identify the stage and worker that are processing the query. The stage id is used to identify the stage of
 * th query execution and the worker id is used to identify the worker that is processing the query. In conjunction with
 * the query id, these three values can be used to uniquely identify a worker processing a query in MSE.
 *
 * It is guaranteed that all server threads running MSE operators will have this context initialized. The same can be
 * said for the server threads running SSE operators spawned by the MSE operators.
 * However, the context is not guaranteed to be initialized in the threads running SSE operators spawned by the SSE.
 */
public abstract class MseWorkerThreadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(MseWorkerThreadContext.class);
  private static final ThreadLocal<Instance> THREAD_LOCAL = new ThreadLocal<>();
  private static final FakeInstance FAKE_INSTANCE = new FakeInstance();

  /**
   * Private constructor to prevent instantiation.
   *
   * Use {@link #open()} to initialize the context instead.
   */
  private MseWorkerThreadContext() {
  }

  private static MseWorkerThreadContext.Instance get() {
    Instance instance = THREAD_LOCAL.get();
    if (instance == null) {
      String errorMessage = "MseThreadContext is not initialized";
      if (QueryThreadContext.isStrictMode()) {
        LOGGER.error(errorMessage);
        throw new IllegalStateException(errorMessage);
      } else {
        LOGGER.debug(errorMessage);
        return FAKE_INSTANCE;
      }
    }
    return instance;
  }

  /**
   * Returns {@code true} if the {@link MseWorkerThreadContext} is initialized in the current thread.
   *
   * Initializing the context means that the {@link #open()} method was called and the returned object is not closed
   * yet.
   */
  public static boolean isInitialized() {
    return THREAD_LOCAL.get() != null;
  }

  /**
   * Captures the state of the {@link MseWorkerThreadContext} in the current thread.
   *
   * This method is used to capture the state of the {@link MseWorkerThreadContext} in the current thread so that it can
   * be restored later in another thread.
   *
   * @return a {@link Memento} object that captures the state of the {@link MseWorkerThreadContext}
   * @throws IllegalStateException if the {@link MseWorkerThreadContext} is not initialized
   */
  public static Memento createMemento() {
    return new Memento(get());
  }

  /**
   * Initializes the {@link MseWorkerThreadContext} with default values.
   *
   * This method will throw an {@link IllegalStateException} if the {@link MseWorkerThreadContext} is already
   * initialized.
   * That indicates an error in the code. Older context must be closed before opening a new one.
   *
   * @return an {@link AutoCloseable} object that should be used within a try-with-resources block
   * @throws IllegalStateException if the {@link MseWorkerThreadContext} is already initialized.
   */
  public static QueryThreadContext.CloseableContext open() {
    return open(null);
  }

  /**
   * Initializes the {@link MseWorkerThreadContext} with the state captured in the given {@link Memento} object, if any.
   *
   * This method will throw an {@link IllegalStateException} if the {@link MseWorkerThreadContext} is already
   * initialized.
   * That indicates an error in the code. Older context must be closed before opening a new one.
   *
   * Values that were set in the {@link Memento} object will be set in the {@link MseWorkerThreadContext} and therefore
   * they couldn't be set again in the current thread (at least until the returned {@link AutoCloseable} is closed).
   *
   * @param memento the {@link Memento} object to capture the state from
   *                (if {@code null}, the context will be initialized with default values)
   * @return an {@link AutoCloseable} object that should be used within a try-with-resources block
   * @throws IllegalStateException if the {@link MseWorkerThreadContext} is already initialized.
   */
  public static QueryThreadContext.CloseableContext open(@Nullable Memento memento) {
    if (THREAD_LOCAL.get() != null) {
      String errorMessage = "MseThreadContext is already initialized";
      if (QueryThreadContext.isStrictMode()) {
        LOGGER.error(errorMessage);
        throw new IllegalStateException(errorMessage);
      } else {
        LOGGER.debug(errorMessage);
        return FAKE_INSTANCE;
      }
    }

    MseWorkerThreadContext.Instance context = new MseWorkerThreadContext.Instance();
    if (memento != null) {
      context.setStageId(memento._stageId);
      context.setWorkerId(memento._workerId);
    }

    THREAD_LOCAL.set(context);

    return context;
  }

  /**
   * Returns a new {@link ExecutorService} whose tasks will be executed with the {@link MseWorkerThreadContext}
   * initialized with the state of the thread submitting the tasks.
   *
   * @param executorService the {@link ExecutorService} to decorate
   */
  public static ExecutorService contextAwareExecutorService(ExecutorService executorService) {
    return contextAwareExecutorService(executorService, MseWorkerThreadContext::createMemento);
  }

  /**
   * Returns a new {@link ExecutorService} whose tasks will be executed with the {@link MseWorkerThreadContext}
   * initialized with the state captured in the given {@link MseWorkerThreadContext.Memento} object.
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
        MseWorkerThreadContext.Memento memento = mementoSupplier.get();
        return () -> {
          try (QueryThreadContext.CloseableContext ignored = open(memento)) {
            return task.call();
          }
        };
      }

      @Override
      protected Runnable decorate(Runnable task) {
        MseWorkerThreadContext.Memento memento = mementoSupplier.get();
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
   * @throws IllegalStateException if the {@link MseWorkerThreadContext} is not initialized
   */
  public static int getStageId() {
    return get().getStageId();
  }

  /**
   * Sets the stage id of the current thread.
   *
   * The stage id can only be set once.
   *
   * @throws IllegalStateException if the stage id is already set or if the {@link MseWorkerThreadContext} is not
   * initialized
   */
  public static void setStageId(int stageId) {
    get().setStageId(stageId);
  }

  /**
   * Returns the worker id of the current thread.
   *
   * The default value is -1.
   * @throws IllegalStateException if the {@link MseWorkerThreadContext} is not initialized
   */
  public static int getWorkerId() {
    return get()._workerId;
  }

  /**
   * Sets the worker id of the current thread.
   *
   * The worker id can only be set once.
   *
   * @throws IllegalStateException if the worker id is already set or if the {@link MseWorkerThreadContext} is not
   * initialized
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

  public static class FakeInstance extends Instance {
    @Override
    public void setStageId(int stageId) {
      LOGGER.debug("Setting stage id to {} in a fake instance", stageId);
    }

    @Override
    public void setWorkerId(int workerId) {
      LOGGER.debug("Setting worker id to {} in a fake instance", workerId);
    }

    @Override
    public void close() {
      // Do nothing
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
