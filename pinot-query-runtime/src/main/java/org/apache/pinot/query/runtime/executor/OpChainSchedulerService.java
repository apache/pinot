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
package org.apache.pinot.query.runtime.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ExecutionList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.MseMeter;
import org.apache.pinot.common.metrics.MseMetrics;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO: Revisit if query cancellation can be handled at higher level
public class OpChainSchedulerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpChainSchedulerService.class);
  private static final int NUM_QUERY_LOCKS = 1 << 10; // 1024 locks
  private static final int QUERY_LOCK_MASK = NUM_QUERY_LOCKS - 1;

  private final String _instanceId;
  /// This [ExecutorService] must be wrapped with [QueryThreadContext#contextAwareExecutorService].
  private final ExecutorService _executorService;
  private final Cache<OpChainId, Pair<MultiStageOperator, QueryExecutionContext>> _opChainCache;
  private final ReadWriteLock[] _queryLocks;
  private final Cache<Long, Boolean> _cancelledQueryCache;
  private final Metrics _metrics = new Metrics();
  /// Per-request opchain completion listeners used by the stream-mode stats-reporting path. A listener is registered
  /// before the broker submits the query and fires once for every opchain that runs on this server for that request.
  /// Listeners are responsible for unregistering themselves once they've consumed all expected events.
  private final ConcurrentMap<Long, OpChainCompletionListener> _completionListeners = new ConcurrentHashMap<>();
  /// Maps requestId → QueryExecutionContext for O(1) cancel. An entry is added when the first opchain for a request
  /// registers and removed when cancel() fires or when the last opchain for that request completes. The counter map
  /// below tracks how many opchains are still active per request so we know when to clean up.
  ///
  /// Consistency invariant: all mutations of BOTH maps for the same requestId must occur inside
  /// `_activeOpChainsByRequest.compute(requestId, …)`. The compute() bin-lock for the requestId key serializes
  /// all register and decrement operations, keeping the two maps coherent. cancel() acquires the query write lock
  /// BEFORE calling compute() so that register()'s read lock cannot overlap with the eviction window.
  ///
  /// NOTE: `_opChainCache.put()` in registerInternal must stay inside the read lock. cancel()'s cache-invalidation
  /// forEach runs OUTSIDE the write lock (after the cancelled-query cache is written). It relies on the read lock
  /// exclusion to guarantee that no register() call is mid-flight between the cache.put and the counter increment
  /// when the forEach observes the cache entry.
  private final ConcurrentMap<Long, QueryExecutionContext> _executionContextByRequest = new ConcurrentHashMap<>();
  private final ConcurrentMap<Long, AtomicInteger> _activeOpChainsByRequest = new ConcurrentHashMap<>();

  public OpChainSchedulerService(String instanceId, ExecutorService executorService, PinotConfiguration config) {
    this(instanceId, executorService, config.getProperty(MultiStageQueryRunner.KEY_OF_OP_STATS_CACHE_SIZE,
            MultiStageQueryRunner.DEFAULT_OF_OP_STATS_CACHE_SIZE),
        config.getProperty(MultiStageQueryRunner.KEY_OF_OP_STATS_CACHE_EXPIRE_MS,
            MultiStageQueryRunner.DEFAULT_OF_OP_STATS_CACHE_EXPIRE_MS),
        config.getProperty(MultiStageQueryRunner.KEY_OF_CANCELLED_QUERY_CACHE_SIZE,
            MultiStageQueryRunner.DEFAULT_OF_CANCELLED_QUERY_CACHE_SIZE),
        config.getProperty(MultiStageQueryRunner.KEY_OF_CANCELLED_QUERY_CACHE_EXPIRE_MS,
            MultiStageQueryRunner.DEFAULT_OF_CANCELLED_QUERY_CACHE_EXPIRE_MS));
  }

  @VisibleForTesting
  public OpChainSchedulerService(ExecutorService executorService) {
    this("testServer", executorService, MultiStageQueryRunner.DEFAULT_OF_OP_STATS_CACHE_SIZE,
        MultiStageQueryRunner.DEFAULT_OF_OP_STATS_CACHE_EXPIRE_MS,
        MultiStageQueryRunner.DEFAULT_OF_CANCELLED_QUERY_CACHE_SIZE,
        MultiStageQueryRunner.DEFAULT_OF_CANCELLED_QUERY_CACHE_EXPIRE_MS);
  }

  private OpChainSchedulerService(String instanceId, ExecutorService executorService, int opStatsCacheSize,
      long opStatsCacheExpireMs, int cancelledQueryCacheSize, long cancelledQueryCacheExpireMs) {
    _instanceId = instanceId;
    _executorService = executorService;
    _opChainCache = CacheBuilder.newBuilder()
        .weigher((OpChainId k, Pair<MultiStageOperator, QueryExecutionContext> v) -> countOperators(v.getLeft()))
        .maximumWeight(opStatsCacheSize)
        .expireAfterWrite(opStatsCacheExpireMs, TimeUnit.MILLISECONDS)
        .build();
    _queryLocks = new ReadWriteLock[NUM_QUERY_LOCKS];
    for (int i = 0; i < NUM_QUERY_LOCKS; i++) {
      _queryLocks[i] = new ReentrantReadWriteLock();
    }
    _cancelledQueryCache = CacheBuilder.newBuilder()
        .maximumSize(cancelledQueryCacheSize)
        .expireAfterWrite(cancelledQueryCacheExpireMs, TimeUnit.MILLISECONDS)
        .build();
  }

  public void register(OpChain operatorChain) {
    QueryExecutionContext executionContext = QueryThreadContext.get().getExecutionContext();
    try {
      // Check before taking the lock as well as inside it.
      checkTermination(operatorChain, executionContext);
      long requestId = operatorChain.getId().getRequestId();
      Lock readLock = getQueryLock(requestId).readLock();
      readLock.lock();
      try {
        checkTermination(operatorChain, executionContext);
        if (_cancelledQueryCache.getIfPresent(requestId) != null) {
          LOGGER.debug("({}): Query has been cancelled", operatorChain);
          executionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "Cancelled on: " + _instanceId);
          throw new QueryCancelledException(
              "Query has been cancelled before op-chain: " + operatorChain.getId() + " being scheduled");
        }
        registerInternal(operatorChain, executionContext);
      } finally {
        readLock.unlock();
      }
    } catch (QueryCancelledException e) {
      try {
        operatorChain.cancel(e);
      } finally {
        operatorChain.close();
      }
      throw e;
    }
  }

  private void checkTermination(OpChain operatorChain, QueryExecutionContext executionContext) {
    TerminationException terminateException = executionContext.getTerminateException();
    if (terminateException != null) {
      LOGGER.debug("({}): Query has been terminated", operatorChain, terminateException);
      if (terminateException.getErrorCode() == QueryErrorCode.QUERY_CANCELLATION) {
        throw new QueryCancelledException(
            "Query has been cancelled before op-chain: " + operatorChain.getId() + " being scheduled");
      } else {
        throw new QueryCancelledException(
            "Query has been terminated before op-chain: " + operatorChain.getId() + " being scheduled: "
                + terminateException.getErrorCode() + " - " + terminateException.getMessage(), terminateException);
      }
    }
  }

  private void registerInternal(OpChain operatorChain, QueryExecutionContext executionContext) {
    OpChainId opChainId = operatorChain.getId();
    long requestId = opChainId.getRequestId();
    MultiStageOperator rootOperator = operatorChain.getRoot();
    _opChainCache.put(opChainId, Pair.of(rootOperator, executionContext));
    // Track the context for O(1) cancel and increment the per-request active opchain count.
    // Both operations are performed inside a single compute() call so that a concurrent decrementActiveOpChains()
    // that observes count==0 and removes the context entry cannot race with a new putIfAbsent arriving between the
    // counter update and the context-map update.
    _activeOpChainsByRequest.compute(requestId, (k, v) -> {
      _executionContextByRequest.putIfAbsent(requestId, executionContext);
      if (v == null) {
        return new AtomicInteger(1);
      }
      v.incrementAndGet();
      return v;
    });
    // Captured by the runJob and read by the FutureCallback so we can hand the calculated stats to the per-request
    // completion listener (stream-mode stats reporting). On error we may not have stats; the FutureCallback handles
    // that by passing null.
    AtomicReference<MultiStageQueryStats> statsRef = new AtomicReference<>();

    OpChainTask task = new OpChainTask(opChainId, new TraceRunnable() {
      @Override
      public void runJob() {
        _metrics.onOpChainStarted();
        LOGGER.trace("({}): Executing", operatorChain);
        MseBlock result = rootOperator.nextBlock();
        while (result.isData()) {
          if (result instanceof ArrowBlock) {
            ((ArrowBlock) result).release();
          }
          result = rootOperator.nextBlock();
        }
        MultiStageQueryStats stats = rootOperator.calculateStats();
        statsRef.set(stats);
        if (result.isError()) {
          ErrorMseBlock errorBlock = (ErrorMseBlock) result;
          throw errorBlock.getMainErrorCode().asException("Error block "
              + "from " + errorBlock.getServerId()
              + ". Msg: " + errorBlock.getErrorMessages()
              + ". Stats: " + stats);
        } else {
          LOGGER.debug("({}): Completed {}", operatorChain, stats);
          _opChainCache.invalidate(opChainId);
        }
      }
    }, new FutureCallback<>() {
      @Override
      public void onSuccess(Void result) {
        finishOpChain(operatorChain, statsRef.get(), null);
      }

      @Override
      public void onFailure(Throwable t) {
        finishOpChain(operatorChain, statsRef.get(), t);
      }
    });

    try {
      // Register the inner task too: cancellation of submit()'s outer future can prevent it from ever running.
      executionContext.addTask(task);
      if (!task.isCancelled()) {
        _executorService.submit(task);
      }
    } catch (RuntimeException e) {
      _opChainCache.invalidate(opChainId);
      task.reject(e);
      throw e;
    }
  }

  private void finishOpChain(OpChain operatorChain, @Nullable MultiStageQueryStats stats, @Nullable Throwable error) {
    OpChainId id = operatorChain.getId();
    try {
      if (error != null) {
        String logMsg = "Failed to execute operator chain: " + error.getMessage();
        if (error instanceof QueryException && ((QueryException) error).getErrorCode() != QueryErrorCode.UNKNOWN
            && ((QueryException) error).getErrorCode() != QueryErrorCode.INTERNAL) {
          LOGGER.warn(logMsg);
        } else {
          LOGGER.error(logMsg, error);
        }
      }
      _metrics.onOpChainFinished(operatorChain.getRoot());
    } finally {
      decrementActiveOpChains(id.getRequestId());
      try {
        notifyCompletionListener(id, operatorChain, stats, error);
      } finally {
        try {
          if (error != null) {
            operatorChain.cancel(error);
          }
        } finally {
          operatorChain.close();
        }
      }
    }
  }

  /**
   * Completes teardown once, either before execution starts or after run() exits. Future cancellation alone does
   * not establish quiescence: a running operator may still be unwinding after the interrupt.
   */
  private static final class OpChainTask extends FutureTask<Void> {
    private final ExecutionList _completion = new ExecutionList();
    private final AtomicBoolean _claimed = new AtomicBoolean();

    private OpChainTask(OpChainId id, Runnable runnable, FutureCallback<Void> callback) {
      super(runnable, null);
      // ExecutionList clears callback captures and reports listener failures without aborting query cancellation.
      _completion.add(() -> {
        switch (state()) {
          case SUCCESS:
            callback.onSuccess(null);
            break;
          case FAILED:
            callback.onFailure(exceptionNow());
            break;
          case CANCELLED:
            callback.onFailure(new QueryCancelledException("Cancelled op-chain: " + id));
            break;
          default:
            throw new IllegalStateException("Op-chain task completed while still running: " + id);
        }
      }, MoreExecutors.directExecutor());
    }

    @Override
    public void run() {
      if (!_claimed.compareAndSet(false, true)) {
        return;
      }
      try {
        super.run();
      } finally {
        _completion.execute();
      }
    }

    @Override
    protected void done() {
      if (_claimed.compareAndSet(false, true)) {
        _completion.execute();
      }
    }

    private void reject(RuntimeException error) {
      setException(error);
    }
  }

  private void decrementActiveOpChains(long requestId) {
    // Use compute() so the "decrement-to-zero → remove" step is atomic with a concurrent registerInternal()
    // that would otherwise interleave a putIfAbsent + increment between our remove() calls.
    _activeOpChainsByRequest.compute(requestId, (k, counter) -> {
      if (counter == null) {
        return null;
      }
      if (counter.decrementAndGet() <= 0) {
        _executionContextByRequest.remove(requestId);
        return null;
      }
      return counter;
    });
  }

  /** Returns the number of requestIds with at least one active opchain. Exposed for tests only. */
  @VisibleForTesting
  int activeRequestCount() {
    return _executionContextByRequest.size();
  }

  private void notifyCompletionListener(OpChainId opChainId, OpChain operatorChain,
      @Nullable MultiStageQueryStats stats, @Nullable Throwable error) {
    OpChainCompletionListener listener = _completionListeners.get(opChainId.getRequestId());
    if (listener == null) {
      return;
    }
    try {
      listener.onOpChainComplete(opChainId, operatorChain.getRoot(), stats, operatorChain.getContext(), error);
    } catch (Throwable listenerError) {
      // The listener is best-effort observability; never let it interfere with opchain teardown.
      LOGGER.warn("OpChain completion listener threw for {}", opChainId, listenerError);
    }
  }

  /**
   * Registers an {@link OpChainCompletionListener} that fires once per opchain finishing for the given request id.
   * Used by the stream-mode stats reporting path. Returns the previously-registered listener, or {@code null} if
   * none — callers should always pass {@code null}.
   */
  @Nullable
  public OpChainCompletionListener registerCompletionListener(long requestId, OpChainCompletionListener listener) {
    return _completionListeners.put(requestId, listener);
  }

  /**
   * Removes any registered listener for the given request id. Idempotent — safe to call even if no listener was
   * registered. Returns the removed listener, or {@code null}.
   */
  @Nullable
  public OpChainCompletionListener unregisterCompletionListener(long requestId) {
    return _completionListeners.remove(requestId);
  }

  /**
   * Cancels all opchains registered for {@code requestId} by terminating the shared
   * {@link QueryExecutionContext} for that request. The cancel is O(1) via a direct context look-up; no
   * per-opchain scan is needed.
   *
   * <p>Stats for cancelled opchains are NOT returned synchronously. In stream mode, each cancelled opchain
   * delivers its (partial) stats asynchronously via {@link OpChainCompletionListener#onOpChainComplete} once the
   * opchain finishes after being interrupted. In legacy mode, cancel-path stats are not collected.
   *
   * <p>If no opchains are currently registered (pre-registration cancel race), the requestId is marked in the
   * cancelled-query cache so that any future registrations for that requestId are immediately rejected.
   */
  public void cancel(long requestId) {
    // Acquire the write lock BEFORE eviction so that register()'s readLock cannot overlap with the window
    // between the context eviction and the cancelled-query cache write. Without the write lock first, a
    // concurrent register() could pass the cache check (cache not yet written) after the context was already
    // evicted, slipping through and starting an opchain that should have been rejected.
    AtomicReference<QueryExecutionContext> ctxRef = new AtomicReference<>();
    Lock writeLock = getQueryLock(requestId).writeLock();
    writeLock.lock();
    try {
      // Atomically evict the counter and the shared QueryExecutionContext inside one compute() so this cannot
      // race with a concurrent registerInternal() that holds the same compute()-bin lock while calling
      // putIfAbsent() on _executionContextByRequest.
      _activeOpChainsByRequest.compute(requestId, (k, counter) -> {
        ctxRef.set(_executionContextByRequest.remove(requestId));
        return null; // unconditionally evict the counter on cancel
      });
      _cancelledQueryCache.put(requestId, Boolean.TRUE);
    } finally {
      writeLock.unlock();
    }
    // Promptly release memory held by cancelled opchain entries; without explicit invalidation they would linger
    // in the cache until TTL expiry. Running this forEach outside the write lock is safe: by the time the write
    // lock was released, the cancelledQueryCache entry was already written, so no new register() calls for this
    // requestId can add entries. Any register() that was already holding the read lock when cancel() began must
    // have completed (it released the read lock for cancel() to acquire the write lock), so its _opChainCache
    // entry is already visible to this iterator. Double-invalidation of an already-evicted entry is a no-op.
    _opChainCache.asMap().forEach((id, pair) -> {
      if (id.getRequestId() == requestId) {
        _opChainCache.invalidate(id);
      }
    });
    QueryExecutionContext context = ctxRef.get();
    if (context != null) {
      // terminate() interrupts all registered tasks (via addTask) and sets the termination flag so that
      // checkTermination() blocks any subsequent registrations for the same requestId.
      context.terminate(QueryErrorCode.QUERY_CANCELLATION, "Cancelled on: " + _instanceId);
    }
  }

  /**
   * Counts the number of operators in the tree rooted at the given operator.
   */
  private int countOperators(MultiStageOperator root) {
    // This stack will have at most 2 elements on most stages given that there is only 1 join in a stage
    // and joins only have 2 children.
    // Some operators (like SetOperator) can have more than 2 children, but they are not common.
    ArrayList<MultiStageOperator> stack = new ArrayList<>(8);
    stack.add(root);
    int result = 0;
    while (!stack.isEmpty()) {
      result++;
      MultiStageOperator operator = stack.remove(stack.size() - 1);
      if (operator.getChildOperators() != null) {
        stack.addAll(operator.getChildOperators());
      }
    }
    return result;
  }

  private ReadWriteLock getQueryLock(long requestId) {
    return _queryLocks[(int) (requestId & QUERY_LOCK_MASK)];
  }

  private static class Metrics {
    private final MseMetrics _mseMetrics = MseMetrics.get();
    private final PinotMeter _startedOpchains = _mseMetrics.getMeteredValue(MseMeter.OPCHAINS_STARTED);
    private final PinotMeter _competedOpchains = _mseMetrics.getMeteredValue(MseMeter.OPCHAINS_COMPLETED);
    private final PinotMeter _emittedRows = _mseMetrics.getMeteredValue(MseMeter.EMITTED_ROWS);
    private final PinotMeter _cpuExecutionTimeMs = _mseMetrics.getMeteredValue(MseMeter.CPU_EXECUTION_TIME_MS);
    private final PinotMeter _memoryAllocatedBytes = _mseMetrics.getMeteredValue(MseMeter.MEMORY_ALLOCATED_BYTES);

    private static final String EMITTED_ROWS = "EMITTED_ROWS";
    private static final String EXECUTION_TIME_MS = "EXECUTION_TIME_MS";
    private static final String ALLOCATED_MEMORY_BYTES = "ALLOCATED_MEMORY_BYTES";

    public void onOpChainStarted() {
      _startedOpchains.mark();
    }

    public void onOpChainFinished(MultiStageOperator rootOperator) {
      _competedOpchains.mark();
      StatMap<?> operatorStats = rootOperator.copyStatMaps();
      _emittedRows.mark(operatorStats.getUnsafe(EMITTED_ROWS, 0L));
      _cpuExecutionTimeMs.mark(operatorStats.getUnsafe(EXECUTION_TIME_MS, 0L));
      _memoryAllocatedBytes.mark(operatorStats.getUnsafe(ALLOCATED_MEMORY_BYTES, 0L));
    }
  }
}
