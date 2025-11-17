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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
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
    // Check if query is already terminated before acquiring the read lock.
    checkTermination(operatorChain, executionContext);
    // Acquire read lock for the query to ensure that the query is not cancelled while scheduling the operator chain.
    long requestId = operatorChain.getId().getRequestId();
    Lock readLock = getQueryLock(requestId).readLock();
    readLock.lock();
    try {
      // Check if query is already terminated again after acquiring the read lock.
      checkTermination(operatorChain, executionContext);
      // Do not schedule the operator chain if the query has been cancelled.
      if (_cancelledQueryCache.getIfPresent(requestId) != null) {
        LOGGER.debug("({}): Query has been cancelled", operatorChain);
        executionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "Cancelled on: " + _instanceId);
        throw new QueryCancelledException(
            "Query has been cancelled before op-chain: " + operatorChain.getId() + " being scheduled");
      } else {
        registerInternal(operatorChain, executionContext);
      }
    } finally {
      readLock.unlock();
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
    MultiStageOperator rootOperator = operatorChain.getRoot();
    _opChainCache.put(opChainId, Pair.of(rootOperator, executionContext));

    // Create a ListenableFutureTask to ensure the opChain is cancelled even if the task is not scheduled
    ListenableFutureTask<Void> listenableFutureTask = ListenableFutureTask.create(new TraceRunnable() {
      @Override
      public void runJob() {
        LOGGER.trace("({}): Executing", operatorChain);
        MseBlock result = rootOperator.nextBlock();
        while (result.isData()) {
          result = rootOperator.nextBlock();
        }
        MultiStageQueryStats stats = rootOperator.calculateStats();
        if (result.isError()) {
          ErrorMseBlock errorBlock = (ErrorMseBlock) result;
          LOGGER.error("({}): Completed erroneously {} {}", operatorChain, stats, errorBlock.getErrorMessages());
          throw new RuntimeException("Got error block: " + errorBlock.getErrorMessages());
        } else {
          LOGGER.debug("({}): Completed {}", operatorChain, stats);
          _opChainCache.invalidate(opChainId);
        }
      }
    }, null);
    Futures.addCallback(listenableFutureTask, new FutureCallback<>() {
      @Override
      public void onSuccess(Void result) {
        operatorChain.close();
      }

      @Override
      public void onFailure(Throwable t) {
        LOGGER.error("({}): Failed to execute operator chain, cancelling", operatorChain, t);
        operatorChain.cancel(t);
        operatorChain.close();
      }
    }, MoreExecutors.directExecutor());

    _executorService.submit(listenableFutureTask);
  }

  public Map<Integer, MultiStageQueryStats.StageStats.Closed> cancel(long requestId) {
    QueryExecutionContext cancelledExecutionContext = null;
    Map<OpChainId, MultiStageOperator> cancelledOperators = new HashMap<>();
    for (Map.Entry<OpChainId, Pair<MultiStageOperator, QueryExecutionContext>> entry : _opChainCache.asMap()
        .entrySet()) {
      if (entry.getKey().getRequestId() == requestId) {
        Pair<MultiStageOperator, QueryExecutionContext> pair = entry.getValue();
        cancelledOperators.put(entry.getKey(), pair.getLeft());
        cancelledExecutionContext = pair.getRight();
      }
    }

    if (cancelledExecutionContext != null) {
      cancelledExecutionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "Cancelled on: " + _instanceId);
      _opChainCache.invalidateAll(cancelledOperators.keySet());
      Map<Integer, MultiStageQueryStats.StageStats.Closed> statsMap = new HashMap<>();
      for (Map.Entry<OpChainId, MultiStageOperator> entry : cancelledOperators.entrySet()) {
        int stageId = entry.getKey().getStageId();
        MultiStageQueryStats.StageStats.Closed stats = entry.getValue().calculateStats().getCurrentStats().close();
        statsMap.merge(stageId, stats, (s1, s2) -> {
          s1.merge(s2);
          return s1;
        });
      }
      return statsMap;
    } else {
      // When no query execution context is found, it means there is no actively running operator chain (registered but
      // not done). To prevent future registration for a cancelled query, add the query to the cancelled query cache.

      // Acquire write lock for the query to ensure that the query is not cancelled while scheduling the operator chain.
      Lock writeLock = getQueryLock(requestId).writeLock();
      writeLock.lock();
      try {
        _cancelledQueryCache.put(requestId, Boolean.TRUE);
      } finally {
        writeLock.unlock();
      }

      return Map.of();
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
}
