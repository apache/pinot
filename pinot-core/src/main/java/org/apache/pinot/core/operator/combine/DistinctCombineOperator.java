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
package org.apache.pinot.core.operator.combine;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for distinct queries.
 */
@SuppressWarnings("rawtypes")
public class DistinctCombineOperator extends BaseSingleBlockCombineOperator<DistinctResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistinctCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_DISTINCT";

  private final AtomicReference<DistinctResultsBlock> _mergedResultsBlock = new AtomicReference<>();
  private final AtomicReference<DistinctResultsBlock> _satisfiedResultsBlock = new AtomicReference<>();
  private final CountDownLatch _latch;

  public DistinctCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
    super(null, operators, queryContext, executorService);
    _latch = new CountDownLatch(_numTasks);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected void processSegments() {
    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      if (_satisfiedResultsBlock.get() != null) {
        return;
      }
      Operator operator = _operators.get(operatorId);
      DistinctResultsBlock resultsBlock;
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        resultsBlock = (DistinctResultsBlock) operator.nextBlock();
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }

      // Use the atomic reference as the swap space to merge the results blocks. If the swap space is null, put the new
      // results block in it. If the swap space is not null, take the results block from the swap space, and merge it
      // with the new results block. Repeat this process until successfully put the new results block in the swap space.
      // After all threads are done, the results block in the swap space is the final merged results block.
      while (true) {
        if (_satisfiedResultsBlock.get() != null) {
          return;
        }
        DistinctTable distinctTable = resultsBlock.getDistinctTable();
        if (distinctTable.isSatisfied()) {
          _satisfiedResultsBlock.compareAndSet(null, resultsBlock);
          return;
        }
        DistinctResultsBlock finalResultsBlock = resultsBlock;
        DistinctResultsBlock mergedResultsBlock =
            _mergedResultsBlock.getAndUpdate(v -> v == null ? finalResultsBlock : null);
        if (mergedResultsBlock == null) {
          break;
        }
        DistinctTable mergedDistinctTable = mergedResultsBlock.getDistinctTable();
        if (mergedDistinctTable.size() >= distinctTable.size()) {
          mergedDistinctTable.mergeDistinctTable(distinctTable);
          resultsBlock = mergedResultsBlock;
        } else {
          distinctTable.mergeDistinctTable(mergedDistinctTable);
        }
      }
    }
  }

  @Override
  protected void onProcessSegmentsException(Throwable t) {
    _processingException.compareAndSet(null, t);
  }

  @Override
  protected void onProcessSegmentsFinish() {
    _latch.countDown();
  }

  @Override
  protected BaseResultsBlock mergeResults()
      throws Exception {
    long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
    boolean opCompleted = _latch.await(timeoutMs, TimeUnit.MILLISECONDS);
    if (!opCompleted) {
      // If this happens, the broker side should already time out. Just log the error and return.
      String errorMessage =
          String.format("Timed out while combining distinct results after %dms, queryContext = %s", timeoutMs,
              _queryContext);
      LOGGER.error(errorMessage);
      return new ExceptionResultsBlock(new TimeoutException(errorMessage));
    }

    DistinctResultsBlock satisfiedResultsBlock = _satisfiedResultsBlock.get();
    if (satisfiedResultsBlock != null) {
      return satisfiedResultsBlock;
    }

    Throwable processingException = _processingException.get();
    if (processingException != null) {
      return new ExceptionResultsBlock(processingException);
    }

    return _mergedResultsBlock.get();
  }
}
