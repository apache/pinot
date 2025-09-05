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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.spi.accounting.ThreadAccounting.DefaultThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.executor.DecoratorExecutorService;
import org.apache.pinot.spi.trace.LoggerConstants;


/// The [QueryThreadContext] class is a thread-local context for storing common query-related information associated to
/// the current thread.
///
/// It is used to pass information between different layers of the query execution stack without changing the method
/// signatures. This is also used to populate the [org.slf4j.MDC] context for logging.
///
/// Use [#open] to initialize the empty context. As any other [AutoCloseable] object, it should be used within a
/// try-with-resources block to ensure the context is properly closed and removed from the thread-local storage and
/// resource usage accountant.
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryThreadContext implements AutoCloseable {
  // Check if query should be terminated, and sample resource usage per 8192 records
  public static final int CHECK_TERMINATION_AND_SAMPLE_USAGE_RECORD_MASK = 0x1FFF;

  private static final ThreadLocal<QueryThreadContext> THREAD_LOCAL = new ThreadLocal<>();

  private final QueryExecutionContext _executionContext;
  private final MseWorkerInfo _mseWorkerInfo;
  private final ThreadResourceUsageAccountant _resourceUsageAccountant;

  private QueryThreadContext(QueryExecutionContext executionContext, @Nullable MseWorkerInfo mseWorkerInfo,
      ThreadResourceUsageAccountant resourceUsageAccountant) {
    _executionContext = executionContext;
    _mseWorkerInfo = mseWorkerInfo;
    _resourceUsageAccountant = resourceUsageAccountant;
    LoggerConstants.REQUEST_ID_KEY.registerInMdc(Long.toString(executionContext.getRequestId()));
    LoggerConstants.CORRELATION_ID_KEY.registerInMdc(executionContext.getCid());
    if (mseWorkerInfo != null) {
      LoggerConstants.STAGE_ID_KEY.registerInMdc(Integer.toString(mseWorkerInfo.getStageId()));
      LoggerConstants.WORKER_ID_KEY.registerInMdc(Integer.toString(mseWorkerInfo.getWorkerId()));
    }
  }

  public QueryExecutionContext getExecutionContext() {
    return _executionContext;
  }

  @Nullable
  public MseWorkerInfo getMseWorkerInfo() {
    return _mseWorkerInfo;
  }

  @JsonIgnore
  public ThreadResourceUsageAccountant getResourceUsageAccountant() {
    return _resourceUsageAccountant;
  }

  private void sampleUsageInternal() {
    _resourceUsageAccountant.sampleUsage();
  }

  private void checkTerminationInternal(Supplier<String> scopeSupplier) {
    checkTerminationInternal(scopeSupplier, _executionContext.getActiveDeadlineMs());
  }

  private void checkTerminationInternal(Supplier<String> scopeSupplier, long deadlineMs) {
    QueryException terminateException = _executionContext.getTerminateException();
    if (terminateException != null) {
      throw terminateException;
    }
    if (Thread.interrupted()) {
      throw new EarlyTerminationException("Interrupted on: " + scopeSupplier.get());
    }
    if (System.currentTimeMillis() >= deadlineMs) {
      throw QueryErrorCode.EXECUTION_TIMEOUT.asException("Timing out on: " + scopeSupplier.get());
    }
  }

  /// Closes the {@link QueryThreadContext} and removes it from the thread-local storage and MDC context.
  @Override
  public void close() {
    _resourceUsageAccountant.clear();
    THREAD_LOCAL.remove();
    LoggerConstants.REQUEST_ID_KEY.unregisterFromMdc();
    LoggerConstants.CORRELATION_ID_KEY.unregisterFromMdc();
    if (_mseWorkerInfo != null) {
      LoggerConstants.STAGE_ID_KEY.unregisterFromMdc();
      LoggerConstants.WORKER_ID_KEY.unregisterFromMdc();
    }
  }

  /// Opens a new [QueryThreadContext] for the current thread and add it to the thread-local storage.
  public static QueryThreadContext open(QueryExecutionContext executionContext,
      ThreadResourceUsageAccountant accountant) {
    return open(executionContext, null, accountant);
  }

  /// Opens a new [QueryThreadContext] for the current thread and add it to the thread-local storage.
  public static QueryThreadContext open(QueryExecutionContext executionContext, @Nullable MseWorkerInfo mseWorkerInfo,
      ThreadResourceUsageAccountant accountant) {
    QueryThreadContext threadContext = new QueryThreadContext(executionContext, mseWorkerInfo, accountant);
    THREAD_LOCAL.set(threadContext);
    accountant.setupTask(threadContext);
    return threadContext;
  }

  @VisibleForTesting
  public static QueryThreadContext openForSseTest() {
    return open(QueryExecutionContext.forSseTest(), DefaultThreadResourceUsageAccountant.INSTANCE);
  }

  @VisibleForTesting
  public static QueryThreadContext openForMseTest() {
    return open(QueryExecutionContext.forMseTest(), new MseWorkerInfo(0, 0),
        DefaultThreadResourceUsageAccountant.INSTANCE);
  }

  /// Returns the [QueryThreadContext] for the current thread.
  public static QueryThreadContext get() {
    QueryThreadContext threadContext = THREAD_LOCAL.get();
    assert threadContext != null;
    return threadContext;
  }

  /// Returns the [QueryThreadContext] for the current thread, or `null` if not available.
  @Nullable
  public static QueryThreadContext getIfAvailable() {
    return THREAD_LOCAL.get();
  }

  /// Returns a new [ExecutorService] whose tasks will be executed with the [QueryThreadContext] initialized with the
  /// state of the thread submitting the tasks.
  public static ExecutorService contextAwareExecutorService(ExecutorService executorService) {
    return new DecoratorExecutorService(executorService, future -> get().getExecutionContext().addTask(future)) {
      @Override
      protected <T> Callable<T> decorate(Callable<T> task) {
        QueryThreadContext parentThreadContext = get();
        return () -> {
          try (QueryThreadContext ignore = open(parentThreadContext._executionContext,
              parentThreadContext._mseWorkerInfo, parentThreadContext._resourceUsageAccountant)) {
            return task.call();
          }
        };
      }

      @Override
      protected Runnable decorate(Runnable task) {
        QueryThreadContext parentThreadContext = get();
        return () -> {
          try (QueryThreadContext ignore = open(parentThreadContext._executionContext,
              parentThreadContext._mseWorkerInfo, parentThreadContext._resourceUsageAccountant)) {
            task.run();
          }
        };
      }
    };
  }

  /// Checks if the query should be terminated.
  public static void checkTermination(Supplier<String> scopeSupplier) {
    QueryThreadContext threadContext = THREAD_LOCAL.get();
    // NOTE: In production code, threadContext should never be null. It might be null in tests when QueryThreadContext
    //       is not set up.
    if (threadContext != null) {
      threadContext.checkTerminationInternal(scopeSupplier);
    }
  }

  /// Checks if the query should be terminated.
  public static void checkTermination(Supplier<String> scopeSupplier, long deadlineMs) {
    QueryThreadContext threadContext = THREAD_LOCAL.get();
    // NOTE: In production code, threadContext should never be null. It might be null in tests when QueryThreadContext
    //       is not set up.
    if (threadContext != null) {
      threadContext.checkTerminationInternal(scopeSupplier, deadlineMs);
    }
  }

  /// Samples the resource usage for the current thread and account it to the query.
  public static void sampleUsage() {
    QueryThreadContext threadContext = THREAD_LOCAL.get();
    // NOTE: In production code, threadContext should never be null. It might be null in tests when QueryThreadContext
    //       is not set up.
    if (threadContext != null) {
    threadContext.sampleUsageInternal();
    }
  }

  /// Checks if the query should be terminated, and samples the resource usage for the current thread and account it
  /// to the query.
  public static void checkTerminationAndSampleUsage(Supplier<String> scopeSupplier) {
    QueryThreadContext threadContext = THREAD_LOCAL.get();
    // NOTE: In production code, threadContext should never be null. It might be null in tests when QueryThreadContext
    //       is not set up.
    if (threadContext != null) {
      threadContext.checkTerminationInternal(scopeSupplier);
      threadContext.sampleUsageInternal();
    }
  }

  /// Checks if the query should be terminated, and samples the resource usage for the current thread and account it
  /// to the query.
  public static void checkTerminationAndSampleUsage(Supplier<String> scopeSupplier, long deadlineMs) {
    QueryThreadContext threadContext = THREAD_LOCAL.get();
    // NOTE: In production code, threadContext should never be null. It might be null in tests when QueryThreadContext
    //       is not set up.
    if (threadContext != null) {
      threadContext.checkTerminationInternal(scopeSupplier, deadlineMs);
      threadContext.sampleUsageInternal();
    }
  }

  /// Checks if the query should be terminated, and samples the resource usage for the current thread and account it
  /// to the query periodically based on the number of records processed.
  public static void checkTerminationAndSampleUsagePeriodically(int numRecordsProcessed,
      Supplier<String> scopeSupplier) {
    if ((numRecordsProcessed & CHECK_TERMINATION_AND_SAMPLE_USAGE_RECORD_MASK) == 0) {
      checkTerminationAndSampleUsage(scopeSupplier);
    }
  }

  /// Checks if the query should be terminated, and samples the resource usage for the current thread and account it
  /// to the query periodically based on the number of records processed.
  public static void checkTerminationAndSampleUsagePeriodically(int numRecordsProcessed, Supplier<String> scopeSupplier,
      long deadlineMs) {
    if ((numRecordsProcessed & CHECK_TERMINATION_AND_SAMPLE_USAGE_RECORD_MASK) == 0) {
      checkTerminationAndSampleUsage(scopeSupplier, deadlineMs);
    }
  }

  public static class MseWorkerInfo {
    private final int _stageId;
    private final int _workerId;

    public MseWorkerInfo(int stageId, int workerId) {
      _stageId = stageId;
      _workerId = workerId;
    }

    public int getStageId() {
      return _stageId;
    }

    public int getWorkerId() {
      return _workerId;
    }
  }
}
