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
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.MseMeter;
import org.apache.pinot.common.metrics.MseMetrics;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
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
  private final ConcurrentMap<Long, QueryExecutionContext> _executionContextByRequest = new ConcurrentHashMap<>();
  private final ConcurrentMap<Long, AtomicInteger> _activeOpChainsByRequest = new ConcurrentHashMap<>();

  public OpChainSchedulerService(String instanceId, ExecutorService executorService, PinotConfiguration config) {
    this(instanceId, executorService,
        config.getProperty(MultiStageQueryRunner.KEY_OF_CANCELLED_QUERY_CACHE_SIZE,
            MultiStageQueryRunner.DEFAULT_OF_CANCELLED_QUERY_CACHE_SIZE),
        config.getProperty(MultiStageQueryRunner.KEY_OF_CANCELLED_QUERY_CACHE_EXPIRE_MS,
            MultiStageQueryRunner.DEFAULT_OF_CANCELLED_QUERY_CACHE_EXPIRE_MS));
  }

  @VisibleForTesting
  public OpChainSchedulerService(ExecutorService executorService) {
    this("testServer", executorService, MultiStageQueryRunner.DEFAULT_OF_CANCELLED_QUERY_CACHE_SIZE,
        MultiStageQueryRunner.DEFAULT_OF_CANCELLED_QUERY_CACHE_EXPIRE_MS);
  }

  private OpChainSchedulerService(String instanceId, ExecutorService executorService, int cancelledQueryCacheSize,
      long cancelledQueryCacheExpireMs) {
    _instanceId = instanceId;
    _executorService = executorService;
    _queryLocks = new ReadWriteLock[NUM_QUERY_LOCKS];
    for (int i = 0; i < NUM_QUERY_LOCKS; i++) {
      _queryLocks[i] = new ReentrantReadWriteLock();
    }
    _cancelledQueryCache = CacheBuilder.newBuilder()
        .maximumSize(cancelledQueryCacheSize)
        .expireAfterWrite(cancelledQueryCacheExpireMs, TimeUnit.MILLISECONDS)
        .build();
  }

  /// Schedules the op chain on the executor.
  ///
  /// If the query is terminated, or was cancelled on this server (which terminates it), the op chain does not run: it
  /// is cancelled with the termination error on the calling thread, which sends that error to its receivers. The op
  /// chain must not be dropped, because its receivers would then wait for data until the query deadline.
  ///
  /// @throws RuntimeException if the executor refuses the op chain
  public void register(OpChain operatorChain) {
    QueryExecutionContext executionContext = QueryThreadContext.get().getExecutionContext();
    // Acquire read lock for the query to ensure that the query is not cancelled while scheduling the operator chain.
    long requestId = operatorChain.getId().getRequestId();
    Lock readLock = getQueryLock(requestId).readLock();
    TerminationException terminateException;
    readLock.lock();
    try {
      if (_cancelledQueryCache.getIfPresent(requestId) != null) {
        LOGGER.debug("({}): Query has been cancelled", operatorChain);
        executionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "Cancelled on: " + _instanceId);
      }
      terminateException = executionContext.getTerminateException();
      if (terminateException == null) {
        registerInternal(operatorChain, executionContext);
        return;
      }
    } finally {
      readLock.unlock();
    }
    // Not scheduled, so that the error does not depend on the executor accepting the op chain
    LOGGER.debug("({}): Query has been terminated", operatorChain, terminateException);
    notifyCompletionListener(operatorChain.getId(), operatorChain, null, terminateException);
    operatorChain.cancel(terminateException);
    operatorChain.close();
  }

  private void registerInternal(OpChain operatorChain, QueryExecutionContext executionContext) {
    OpChainId opChainId = operatorChain.getId();
    long requestId = opChainId.getRequestId();
    MultiStageOperator rootOperator = operatorChain.getRoot();
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

    // The FutureCallback below runs on the thread that runs the op chain, once the op chain completes
    ListenableFutureTask<Void> listenableFutureTask = ListenableFutureTask.create(new TraceRunnable() {
      @Override
      public void runJob() {
        _metrics.onOpChainStarted();
        // The query was terminated while the op chain waited for a thread. Fail without doing any work: the
        // FutureCallback then cancels the op chain with the termination error, which sends that error to the receivers.
        TerminationException terminateException = executionContext.getTerminateException();
        if (terminateException != null) {
          throw terminateException;
        }
        LOGGER.trace("({}): Executing", operatorChain);
        MseBlock result = rootOperator.nextBlock();
        while (result.isData()) {
          result = rootOperator.nextBlock();
        }
        MultiStageQueryStats stats = rootOperator.calculateStats();
        statsRef.set(stats);
        if (result.isError()) {
          ErrorMseBlock errorBlock = (ErrorMseBlock) result;
          String serverId = errorBlock.getServerId() != null ? errorBlock.getServerId() : "unknown";
          throw errorBlock.getMainErrorCode().asException("Error block "
              + "from stage " + errorBlock.getStageId()
              + " worker " + errorBlock.getWorkerId()
              + " on " + serverId
              + ". Msg: " + errorBlock.getErrorMessages()
              + ". Stats: " + stats);
        } else {
          LOGGER.debug("({}): Completed {}", operatorChain, stats);
        }
      }
    }, null);
    Futures.addCallback(listenableFutureTask, new FutureCallback<>() {
      @Override
      public void onSuccess(Void result) {
        _metrics.onOpChainFinished(rootOperator);
        decrementActiveOpChains(requestId);
        notifyCompletionListener(opChainId, operatorChain, statsRef.get(), null);
        operatorChain.close();
      }

      @Override
      public void onFailure(Throwable t) {
        String logMsg = "Failed to execute operator chain: " + t.getMessage();
        _metrics.onOpChainFinished(rootOperator);
        switch (QueryErrorCode.fromThrowable(t, QueryErrorCode.UNKNOWN)) {
          case UNKNOWN:
          case INTERNAL:
            LOGGER.error(logMsg, t);
            break;
          default:
            LOGGER.warn(logMsg);
            break;
        }
        decrementActiveOpChains(requestId);
        notifyCompletionListener(opChainId, operatorChain, statsRef.get(), t);
        operatorChain.cancel(t);
        operatorChain.close();
      }
    }, MoreExecutors.directExecutor());

    OpChainTask opChainTask = new OpChainTask(listenableFutureTask);
    try {
      // Use execute() and not submit(): submit() registers its Future with the execution context, and terminating the
      // query would cancel that Future, which drops the op chain if it has not started yet. See OpChainTask.
      _executorService.execute(opChainTask);
    } catch (RuntimeException e) {
      // The MSE executor is wrapped in HardLimitExecutor + heap throttling, so execute() can throw (e.g.
      // IllegalStateException when the limit is reached). When it does, the task never runs, so the directExecutor
      // FutureCallback above (which decrements the active-opchain counter and removes the per-request context entry)
      // never fires. Back that bookkeeping out here so the per-request context entry does not leak until a later
      // cancel, then rethrow so the caller propagates the failure as a stage error. The caller,
      // QueryRunner#processQueryBlocking, close()s the op chain on this rethrow, releasing operator resources that the
      // never-fired FutureCallback would otherwise have closed.
      decrementActiveOpChains(requestId);
      throw e;
    }
    // Registered after execute(), so that a task the executor refuses is not kept by the context. If the query is
    // terminated in between, addTask() cancels the task right away.
    executionContext.addTask(opChainTask);
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

  /// Returns the number of requestIds with at least one active opchain. Exposed for tests only.
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

  /// Registers an [OpChainCompletionListener] that fires once per opchain finishing for the given request id.
  /// Used by the stream-mode stats reporting path. Returns the previously-registered listener, or `null` if
  /// none — callers should always pass `null`.
  @Nullable
  public OpChainCompletionListener registerCompletionListener(long requestId, OpChainCompletionListener listener) {
    return _completionListeners.put(requestId, listener);
  }

  /// Removes any registered listener for the given request id. Idempotent — safe to call even if no listener was
  /// registered. Returns the removed listener, or `null`.
  @Nullable
  public OpChainCompletionListener unregisterCompletionListener(long requestId) {
    return _completionListeners.remove(requestId);
  }

  /// Cancels all opchains registered for `requestId` by terminating the shared
  /// [QueryExecutionContext] for that request. The cancel is O(1) via a direct context look-up; no
  /// per-opchain scan is needed.
  ///
  /// Stats for cancelled opchains are NOT returned synchronously. In stream mode, each cancelled opchain
  /// delivers its (partial) stats asynchronously via [OpChainCompletionListener#onOpChainComplete] once the
  /// opchain finishes after being interrupted. In legacy mode, cancel-path stats are not collected.
  ///
  /// If no opchains are currently registered (pre-registration cancel race), the requestId is marked in the
  /// cancelled-query cache so that the op chains registered later for that requestId fail without running.
  public void cancel(long requestId) {
    // Acquire the write lock BEFORE eviction so that register()'s readLock cannot overlap with the window
    // between the context eviction and the cancelled-query cache write. Without the write lock first, a
    // concurrent register() could pass the cache check (cache not yet written) after the context was already
    // evicted, slipping through and running an opchain of a cancelled query.
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
    QueryExecutionContext context = ctxRef.get();
    if (context != null) {
      // terminate() interrupts all registered tasks (via addTask) and sets the termination flag, so that op chains
      // that have not started yet, and the ones registered later for the same requestId, fail without running.
      context.terminate(QueryErrorCode.QUERY_CANCELLATION, "Cancelled on: " + _instanceId);
    }
  }

  private ReadWriteLock getQueryLock(long requestId) {
    return _queryLocks[(int) (requestId & QUERY_LOCK_MASK)];
  }

  /// Runs an op chain. It is registered with the [QueryExecutionContext] of the query, so that terminating the query
  /// interrupts the op chain.
  ///
  /// [QueryExecutionContext#terminate] cancels the registered tasks. A `Future` from [ExecutorService#submit] that is
  /// cancelled before its task starts never runs the task. An op chain dropped this way never sends an EOS or an error
  /// to its receivers, so they wait for data until the query deadline. Cancelling this task only interrupts the op
  /// chain while it runs. An op chain that has not started yet still runs, and fails on its first check: the context
  /// sets the termination error before it cancels the tasks.
  ///
  /// `_lock` makes sure that an interrupt reaches the thread only while it runs the op chain, and never the next task
  /// of that thread.
  private static final class OpChainTask extends AbstractFuture<Void> implements Runnable {
    private final Runnable _opChainRunner;
    private final Object _lock = new Object();
    @GuardedBy("_lock")
    @Nullable
    private Thread _thread;

    OpChainTask(Runnable opChainRunner) {
      _opChainRunner = opChainRunner;
    }

    @Override
    public void run() {
      synchronized (_lock) {
        _thread = Thread.currentThread();
      }
      try {
        _opChainRunner.run();
      } finally {
        synchronized (_lock) {
          _thread = null;
        }
        // _thread is null, so interruptTask() can no longer interrupt this thread. Clear an interrupt that it sent
        // earlier, after the op chain stopped checking for interrupts.
        Thread.interrupted();
        set(null);
      }
    }

    @Override
    protected void interruptTask() {
      synchronized (_lock) {
        if (_thread != null) {
          _thread.interrupt();
        }
      }
    }
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
