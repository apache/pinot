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
package org.apache.pinot.spi.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.executor.DecoratorExecutorService;
import org.apache.pinot.spi.trace.LoggerConstants;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The {@code QueryThreadContext} class is a thread-local context for storing common query-related information
 * associated to the current thread.
 *
 * <p>It is used to pass information between different layers of the query execution stack without changing the
 * method signatures. This is also used to populate the {@link MDC} context for logging.
 *
 * Use {@link #open()} to initialize the empty context. As any other {@link AutoCloseable} object, it should be used
 * within a try-with-resources block to ensure the context is properly closed and removed from the thread-local storage.
 *
 * Sometimes it is necessary to copy the state of the {@link QueryThreadContext} from one thread to another. In this
 * case, use the {@link #createMemento()} method to capture the state of the {@link QueryThreadContext} in the current
 * thread and then use the {@link #open(Memento)} method to initialize the context in the target thread with the state
 * captured in the {@link Memento} object. The API may be a bit cumbersome, but it ensures that the state is always
 * copied between threads in a safe way and makes it impossible to use the {@link QueryThreadContext} from another
 * thread by mistake.
 *
 * It is guaranteed that all server and broker threads running SSE and MSE will have this context initialized as soon
 * as the request is received. Ingestion threads that use the query execution stack will also have this context
 * initialized.
 */
public class QueryThreadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryThreadContext.class);
  private static final ThreadLocal<Instance> THREAD_LOCAL = new ThreadLocal<>();
  public static volatile boolean _strictMode = false;
  private static final FakeInstance FAKE_INSTANCE = new FakeInstance();

  static {
    // This is a hack to know if assertions are enabled or not
    boolean assertEnabled = false;
    //CHECKSTYLE:OFF
    assert assertEnabled = true;
    //CHECKSTYLE:ON
    _strictMode = assertEnabled;
  }

  /**
   * Private constructor to prevent instantiation.
   *
   * Use {@link #open()} to initialize the context instead.
   */
  private QueryThreadContext() {
  }

  /**
   * Sets the strict mode of the {@link QueryThreadContext} from the given configuration.
   */
  public static void onStartup(PinotConfiguration conf) {
    String mode = conf.getProperty(CommonConstants.Query.CONFIG_OF_QUERY_CONTEXT_MODE);
    if ("strict".equalsIgnoreCase(mode)) {
      _strictMode = true;
    }
    if (mode != null && !mode.isEmpty()) {
      throw new IllegalArgumentException("Invalid value '" + mode + "' for "
          + CommonConstants.Query.CONFIG_OF_QUERY_CONTEXT_MODE + ". Expected 'strict' or empty");
    }
  }

  /**
   * Returns {@code true} if the {@link QueryThreadContext} is in strict mode.
   *
   * In strict mode, if the {@link QueryThreadContext} is not initialized, an {@link IllegalStateException} will be
   * thrown when setter and getter methods are used.
   * In non-strict mode, a warning will be logged and the fake instance will be returned.
   *
   * @see #onStartup(PinotConfiguration)
   */
  public static boolean isStrictMode() {
    return _strictMode;
  }

  private static Instance get() {
    Instance instance = THREAD_LOCAL.get();
    if (instance == null) {
      String errorMessage = "QueryThreadContext is not initialized";
      if (_strictMode) {
        LOGGER.error(errorMessage);
        throw new IllegalStateException("QueryThreadContext is not initialized");
      } else {
        LOGGER.debug(errorMessage);
        // in non-strict mode, return the fake instance
        return FAKE_INSTANCE;
      }
    }
    return instance;
  }

  /**
   * Returns {@code true} if the {@link QueryThreadContext} is initialized in the current thread.
   *
   * Initializing the context means that the {@link #open()} method was called and the returned object is not closed
   * yet.
   */
  public static boolean isInitialized() {
    return THREAD_LOCAL.get() != null;
  }

  /**
   * Captures the state of the {@link QueryThreadContext} in the current thread.
   *
   * This method is used to capture the state of the {@link QueryThreadContext} in the current thread so that it can be
   * restored later in another thread.
   *
   * @return a {@link Memento} object that captures the state of the {@link QueryThreadContext}
   * @throws IllegalStateException if the {@link QueryThreadContext} is not initialized
   * @see #open(Memento)
   */
  public static Memento createMemento() {
    return new Memento(get());
  }

  /**
   * Initializes the {@link QueryThreadContext} with default values.
   *
   * This method will throw an {@link IllegalStateException} if the {@link QueryThreadContext} is already initialized.
   * That indicates an error in the code. Older context must be closed before opening a new one.
   *
   * @return an {@link AutoCloseable} object that should be used within a try-with-resources block
   * @throws IllegalStateException if the {@link QueryThreadContext} is already initialized.
   */
  public static CloseableContext open() {
    return open(null);
  }

  public static CloseableContext openFromRequestMetadata(Map<String, String> requestMetadata) {
    CloseableContext open = open();
    String cid = requestMetadata.get(CommonConstants.Query.Request.MetadataKeys.CORRELATION_ID);
    long requestId = Long.parseLong(requestMetadata.get(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID));
    if (cid == null) {
      cid = Long.toString(requestId);
    }
    QueryThreadContext.setIds(requestId, cid);
    long timeoutMs = Long.parseLong(requestMetadata.get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS));
    long startTimeMs = System.currentTimeMillis();
    QueryThreadContext.setStartTimeMs(startTimeMs);
    QueryThreadContext.setDeadlineMs(startTimeMs + timeoutMs);

    return open;
  }

  /**
   * Initializes the {@link QueryThreadContext} with the state captured in the given {@link Memento} object, if any.
   *
   * This method will throw an {@link IllegalStateException} if the {@link QueryThreadContext} is already initialized.
   * That indicates an error in the code. Older context must be closed before opening a new one.
   *
   * Values that were set in the {@link Memento} object will be set in the {@link QueryThreadContext} and therefore
   * they couldn't be set again in the current thread (at least until the returned {@link AutoCloseable} is closed).
   *
   * @param memento the {@link Memento} object to capture the state from
   *                (if {@code null}, the context will be initialized with default values)
   * @return an {@link AutoCloseable} object that should be used within a try-with-resources block
   * @throws IllegalStateException if the {@link QueryThreadContext} is already initialized.
   */
  public static CloseableContext open(@Nullable Memento memento) {
    if (THREAD_LOCAL.get() != null) {
      String errorMessage = "QueryThreadContext is already initialized";
      if (_strictMode) {
        LOGGER.error(errorMessage);
        throw new IllegalStateException("QueryThreadContext is already initialized");
      } else {
        LOGGER.debug(errorMessage);
        return FAKE_INSTANCE;
      }
    }

    Instance context = new Instance();
    if (memento != null) {
      context.setStartTimeMs(memento._startTimeMs);
      context.setDeadlineMs(memento._deadlineMs);
      context.setBrokerId(memento._brokerId);
      context.setRequestId(memento._requestId);
      context.setCid(memento._cid);
      context.setSql(memento._sql);
      context.setQueryEngine(memento._queryEngine);
    }

    THREAD_LOCAL.set(context);

    return context;
  }

  /**
   * Returns a new {@link ExecutorService} whose tasks will be executed with the {@link QueryThreadContext} initialized
   * with the state of the thread submitting the tasks.
   *
   * @param executorService the {@link ExecutorService} to decorate
   */
  public static ExecutorService contextAwareExecutorService(ExecutorService executorService) {
    return contextAwareExecutorService(executorService, QueryThreadContext::createMemento);
  }

  /**
   * Returns a new {@link ExecutorService} whose tasks will be executed with the {@link QueryThreadContext} initialized
   * with the state captured in the given {@link Memento} object.
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
        Memento memento = mementoSupplier.get();
        return () -> {
          try (CloseableContext ignored = open(memento)) {
            return task.call();
          }
        };
      }

      @Override
      protected Runnable decorate(Runnable task) {
        Memento memento = mementoSupplier.get();
        return () -> {
          try (CloseableContext ignored = open(memento)) {
            task.run();
          }
        };
      }
    };
  }

  /**
   * Returns the start time of the query in milliseconds.
   *
   * The default value of 0 means the start time is not set.
   * @throws IllegalStateException if the {@link QueryThreadContext} is not initialized
   */
  public static long getStartTimeMs() {
    return get().getStartTimeMs();
  }

  /**
   * Sets the start time of the query in milliseconds since epoch.
   *
   * The start time can only be set once.
   * @throws IllegalStateException if start time is already set or if the {@link QueryThreadContext} is not initialized
   */
  public static void setStartTimeMs(long startTimeMs) {
    get().setStartTimeMs(startTimeMs);
  }

  /**
   * Returns the deadline time of the query in milliseconds since epoch.
   *
   * The default value of 0 means the deadline is not set.
   * @throws IllegalStateException if the {@link QueryThreadContext} is not initialized
   */
  public static long getDeadlineMs() {
    return get().getDeadlineMs();
  }

  /**
   * Sets the deadline time of the query in milliseconds since epoch.
   *
   * The deadline can only be set once.
   * @throws IllegalStateException if deadline is already set or if the {@link QueryThreadContext} is not initialized
   */
  public static void setDeadlineMs(long deadlineMs) {
    get().setDeadlineMs(deadlineMs);
  }

  /**
   * Returns the timeout of the query in milliseconds.
   *
   * The default value of 0 means the timeout is not set.
   * @throws IllegalStateException if the {@link QueryThreadContext} is not initialized
   */
  public static String getBrokerId() {
    return get().getBrokerId();
  }

  /**
   * Sets the broker id of the query.
   *
   * The broker id can only be set once.
   * @throws IllegalStateException if broker id is already set or if the {@link QueryThreadContext} is not initialized
   */
  public static void setBrokerId(String brokerId) {
    get().setBrokerId(brokerId);
  }

  /**
   * Returns the request id of the query.
   *
   * The request id is used to identify query phases across different systems.
   * Contrary to the correlation id, a single logical query can have multiple request ids.
   * This is because the request id is changed at different stages of the query (e.g. real-time and offline parts of
   * the query or different leaf operations in MSE).
   *
   * Also remember that neither request nor correlation id are guaranteed to be unique.
   *
   * The default value of 0 means the request id is not set.
   * @throws IllegalStateException if the {@link QueryThreadContext} is not initialized
   */
  public static long getRequestId() {
    return get().getRequestId();
  }

  /**
   * Returns the correlation id of the query.
   *
   * Correlation id is used to track queries across different systems.
   * This id can be supplied by the client or generated by the system (usually the broker). It is not guaranteed to be
   * unique. Users can use the same correlation id for multiple queries to track a single logical workflow on their
   * side.
   *
   * What is guaranteed is that the correlation id will be consistent across all the phases of the query. Remember that
   * it is not the case for the request id, which can change at different stages of the query (e.g. real-time and
   * offline parts of the query or different leaf operations in MSE).
   *
   * Also remember that neither request nor correlation id are guaranteed to be unique.
   *
   * The default value of {@code null} means the correlation id is not set.
   * @throws IllegalStateException if the {@link QueryThreadContext} is not initialized
   */
  public static String getCid() {
    return get().getCid();
  }

  /**
   * Sets the request id and correlation id of the query.
   *
   * Both requests can only be set once.
   *
   * Setting this value also registers it in the MDC context with the key {@link LoggerConstants#REQUEST_ID_KEY} and
   * {@link LoggerConstants#CORRELATION_ID_KEY}.
   * @throws IllegalStateException if any of the ids are already set or if the {@link QueryThreadContext} is not
   * initialized
   */
  public static void setIds(long requestId, String cid) {
    Instance instance = get();
    instance.setRequestId(requestId);
    instance.setCid(cid);
  }

  /**
   * Returns the SQL of the query.
   *
   * The default value of {@code null} means the SQL is not set.
   * @throws IllegalStateException if the {@link QueryThreadContext} is not initialized
   */
  public static String getSql() {
    return get().getSql();
  }

  /**
   * Sets the SQL of the query.
   *
   * The SQL can only be set once.
   * @throws IllegalStateException if sql is already set or if the {@link QueryThreadContext} is not initialized
   */
  public static void setSql(String sql) {
    get().setSql(sql);
  }

  /**
   * Returns the query engine that is being used.
   *
   * The default value of {@code null} means the query engine is not set.
   * @throws IllegalStateException if the {@link QueryThreadContext} is not initialized
   */
  public static String getQueryEngine() {
    return get().getQueryEngine();
  }

  /**
   * Sets the query engine that is being used.
   *
   * The query engine can only be set once.
   * @throws IllegalStateException if query engine is already set or if the {@link QueryThreadContext} is not
   * initialized
   */
  public static void setQueryEngine(String queryEngine) {
    get().setQueryEngine(queryEngine);
  }

  /**
   * This private class stores the actual state of the {@link QueryThreadContext} in a safe way.
   *
   * As part of the paranoid design of the {@link QueryThreadContext}, this class is used to store the state of the
   * {@link QueryThreadContext} in a way that is safe to copy between threads.
   * Callers never have access to this class directly, but only through the {@link QueryThreadContext} static methods.
   * This is designed this way to ensure that the state is always copied between threads in a safe way.
   * Given it is impossible for a caller to access this class directly, it can never be copied from one thread to
   * another by mistake.
   * This forces to use the {@link Memento} class to capture the state of the {@link QueryThreadContext} in the current
   * thread and then use the {@link #open(Memento)} method to initialize the context in the target thread with the state
   * captured in the {@link Memento} object.
   */
  private static class Instance implements CloseableContext {
    private long _startTimeMs;
    private long _deadlineMs;
    private String _brokerId;
    private long _requestId;
    private String _cid;
    private String _sql;
    private String _queryEngine;

    public long getStartTimeMs() {
      return _startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs) {
      Preconditions.checkState(getStartTimeMs() == 0, "Start time already set to %s, cannot set again",
          getStartTimeMs());
      _startTimeMs = startTimeMs;
    }

    public long getDeadlineMs() {
      return _deadlineMs;
    }

    public void setDeadlineMs(long deadlineMs) {
      Preconditions.checkState(getDeadlineMs() == 0, "Deadline already set to %s, cannot set again",
          getDeadlineMs());
      _deadlineMs = deadlineMs;
    }

    public String getBrokerId() {
      return _brokerId;
    }

    public void setBrokerId(String brokerId) {
      Preconditions.checkState(getBrokerId() == null, "Broker id already set to %s, cannot set again",
          getBrokerId());
      _brokerId = brokerId;
    }

    public long getRequestId() {
      return _requestId;
    }

    public void setRequestId(long requestId) {
      Preconditions.checkState(getRequestId() == 0, "Request id already set to %s, cannot set again", getRequestId());
      LoggerConstants.REQUEST_ID_KEY.registerInMdc(Long.toString(requestId));
      _requestId = requestId;
    }

    public String getCid() {
      return _cid;
    }

    public void setCid(String cid) {
      Preconditions.checkState(getCid() == null, "Correlation id already set to %s, cannot set again",
          getCid());
      LoggerConstants.CORRELATION_ID_KEY.registerInMdc(cid);
      _cid = cid;
    }

    public String getSql() {
      return _sql;
    }

    public void setSql(String sql) {
      Preconditions.checkState(getSql() == null, "SQL already set to %s, cannot set again",
          getSql());
      _sql = sql;
    }

    public String getQueryEngine() {
      return _queryEngine;
    }

    public void setQueryEngine(String queryType) {
      Preconditions.checkState(getQueryEngine() == null, "Query type already set to %s, cannot set again",
          getQueryEngine());
      _queryEngine = queryType;
    }

    @Override
    public String toString() {
      try {
        return JsonUtils.objectToString(this);
      } catch (JsonProcessingException e) {
        return "Failed to convert QueryThreadContext to JSON: " + e.getMessage();
      }
    }

    /**
     * Closes the {@link QueryThreadContext} and removes it from the thread-local storage and MDC context.
     */
    @Override
    public void close() {
      THREAD_LOCAL.remove();
      if (_requestId != 0) {
        LoggerConstants.REQUEST_ID_KEY.unregisterFromMdc();
      }
      if (_cid != null) {
        LoggerConstants.CORRELATION_ID_KEY.unregisterFromMdc();
      }
    }
  }

  private static class FakeInstance extends Instance {
    @Override
    public void setStartTimeMs(long startTimeMs) {
      LOGGER.debug("Setting start time to {} in a fake context", startTimeMs);
    }

    @Override
    public void setDeadlineMs(long deadlineMs) {
      LOGGER.debug("Setting deadline to {} in a fake context", deadlineMs);
    }

    @Override
    public void setBrokerId(String brokerId) {
      LOGGER.debug("Setting broker id to {} in a fake context", brokerId);
    }

    @Override
    public void setRequestId(long requestId) {
      LOGGER.debug("Setting request id to {} in a fake context", requestId);
    }

    @Override
    public void setCid(String cid) {
      LOGGER.debug("Setting correlation id to {} in a fake context", cid);
    }

    @Override
    public void setSql(String sql) {
      LOGGER.debug("Setting SQL to {} in a fake context", sql);
    }

    @Override
    public void setQueryEngine(String queryType) {
      LOGGER.debug("Setting query type to {} in a fake context", queryType);
    }

    @Override
    public void close() {
      // Do nothing
    }
  }

  /**
   * The {@code Memento} class is used to capture the state of the {@link QueryThreadContext} for saving and restoring
   * the state.
   *
   * Although the <a href="https://en.wikipedia.org/wiki/Memento_pattern">Memento Design Pattern</a> is usually used to
   * keep state private, here we are using it to be sure {@link QueryThreadContext} can only be copied from threads in
   * a safe way.
   *
   * Given the only way to create a {@link QueryThreadContext} with known state is through the {@link Memento} class,
   * we can be sure that the state is always copied between threads. The alternative would be to create an
   * {@link #open()} method that accepts a {@link QueryThreadContext} as an argument, but that would allow the receiver
   * thread to use the received {@link QueryThreadContext} directly, which is not safe because it belongs to another
   * thread.
   */
  public static class Memento {
    private final long _startTimeMs;
    private final long _deadlineMs;
    private final String _brokerId;
    private final long _requestId;
    private final String _cid;
    private final String _sql;
    private final String _queryEngine;

    private Memento(Instance instance) {
      _startTimeMs = instance.getStartTimeMs();
      _deadlineMs = instance.getDeadlineMs();
      _brokerId = instance.getBrokerId();
      _requestId = instance.getRequestId();
      _cid = instance.getCid();
      _sql = instance.getSql();
      _queryEngine = instance.getQueryEngine();
    }
  }

  public interface CloseableContext extends AutoCloseable {
    @Override
    void close();
  }
}
