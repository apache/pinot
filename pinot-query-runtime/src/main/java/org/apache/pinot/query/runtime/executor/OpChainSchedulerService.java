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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpChainSchedulerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpChainSchedulerService.class);

  private final ExecutorService _executorService;
  private final ConcurrentHashMap<OpChainId, Future<?>> _submittedOpChainMap = new ConcurrentHashMap<>();
  private final Cache<OpChainId, MultiStageOperator> _opChainCache;


  public OpChainSchedulerService(ExecutorService executorService, PinotConfiguration config) {
    this(
        executorService,
        config.getProperty(MultiStageQueryRunner.KEY_OF_OP_STATS_CACHE_MAX,
            MultiStageQueryRunner.DEFAULT_OF_OP_STATS_CACHE_MAX),
        config.getProperty(MultiStageQueryRunner.KEY_OF_OP_STATS_CACHE_EXPIRE_MS,
            MultiStageQueryRunner.DEFAULT_OF_OP_STATS_CACHE_EXPIRE_MS)
    );
  }

  public OpChainSchedulerService(ExecutorService executorService) {
    this(executorService, MultiStageQueryRunner.DEFAULT_OF_OP_STATS_CACHE_MAX,
        MultiStageQueryRunner.DEFAULT_OF_OP_STATS_CACHE_EXPIRE_MS);
  }

  public OpChainSchedulerService(ExecutorService executorService, int maxWeight, long expireAfterWriteMs) {
    _executorService = executorService;
    _opChainCache = CacheBuilder.newBuilder()
        .weigher((OpChainId key, MultiStageOperator value) -> countOperators(value))
        .maximumWeight(maxWeight)
        .expireAfterWrite(expireAfterWriteMs, TimeUnit.MILLISECONDS)
        .build();
  }

  public void register(OpChain operatorChain) {
    _opChainCache.put(operatorChain.getId(), operatorChain.getRoot());

    Future<?> scheduledFuture = _executorService.submit(new TraceRunnable() {
      @Override
      public void runJob() {
        ErrorMseBlock errorBlock = null;
        Throwable thrown = null;
        // try-with-resources to ensure that the operator chain is closed
        // TODO: Change the code so we ownership is expressed in the code in a better way
        try (OpChain closeMe = operatorChain) {
          Tracing.ThreadAccountantOps.setupWorker(operatorChain.getId().getStageId(),
              ThreadExecutionContext.TaskType.MSE, operatorChain.getParentContext());
          LOGGER.trace("({}): Executing", operatorChain);
          MseBlock result = operatorChain.getRoot().nextBlock();
          while (result.isData()) {
            result = operatorChain.getRoot().nextBlock();
          }
          MultiStageQueryStats stats = operatorChain.getRoot().calculateStats();
          if (result.isError()) {
            errorBlock = (ErrorMseBlock) result;
            LOGGER.error("({}): Completed erroneously {} {}", operatorChain, stats, errorBlock.getErrorMessages());
          } else {
            LOGGER.debug("({}): Completed {}", operatorChain, stats);
            _opChainCache.invalidate(operatorChain.getId());
          }
        } catch (Exception e) {
          LOGGER.error("({}): Failed to execute operator chain!", operatorChain, e);
          thrown = e;
        } finally {
          _submittedOpChainMap.remove(operatorChain.getId());
          if (errorBlock != null || thrown != null) {
            if (thrown == null) {
              thrown = new RuntimeException("Error block " + errorBlock.getErrorMessages());
            }
            operatorChain.cancel(thrown);
          }
          Tracing.ThreadAccountantOps.clear();
        }
      }
    });
    _submittedOpChainMap.put(operatorChain.getId(), scheduledFuture);
  }

  public Map<Integer, MultiStageQueryStats.StageStats.Closed> cancel(long requestId) {
    // simple cancellation. for leaf stage this cannot be a dangling opchain b/c they will eventually be cleared up
    // via query timeout.
    Iterator<Map.Entry<OpChainId, Future<?>>> iterator = _submittedOpChainMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<OpChainId, Future<?>> entry = iterator.next();
      if (entry.getKey().getRequestId() == requestId) {
        entry.getValue().cancel(true);
        iterator.remove();
      }
    }
    Map<OpChainId, MultiStageOperator> cancelledByOpChainId = _opChainCache.asMap()
        .entrySet()
        .stream()
        .filter(entry -> entry.getKey().getRequestId() == requestId)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1));
    _opChainCache.invalidateAll(cancelledByOpChainId.keySet());

    return cancelledByOpChainId.entrySet()
        .stream()
        .collect(Collectors.toMap(
            e -> e.getKey().getStageId(),
            e -> e.getValue().calculateStats().getCurrentStats().close(),
            (e1, e2) -> {
              e1.merge(e2);
              return e1;
            }
        ));
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
}
