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
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Non-blocking combine operator for group-by queries.
 *
 * <p>Each worker thread builds its own thread-local {@link org.apache.pinot.core.data.table.SimpleIndexedTable}
 * (uncontended {@code HashMap}) and processes segments without any cross-thread contention during accumulation.
 * After all segments are processed, threads merge their local tables into a single result via a lock-free tournament
 * exchange protocol: each thread tries to claim the empty shared slot, or steals the current occupant and merges
 * (larger absorbs smaller), looping until the winning table is deposited.
 *
 * <p>Parallelism is bounded by the configured max execution threads via {@link GroupByCombineOperator}.
 */
@SuppressWarnings("rawtypes")
public class NonblockingGroupByCombineOperator extends GroupByCombineOperator {
  public static final String ALGORITHM = "NON-BLOCKING";

  private static final Logger LOGGER = LoggerFactory.getLogger(NonblockingGroupByCombineOperator.class);

  public NonblockingGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(operators, queryContext, executorService);
    LOGGER.debug("Using {} for group-by combine with {} tasks", ALGORITHM, _numTasks);
  }

  /**
   * Processes all assigned segments into a thread-local indexed table, then deposits it into the shared slot via the
   * lock-free tournament exchange.
   */
  @Override
  protected void processSegments() {
    int operatorId;
    IndexedTable indexedTable = null;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        GroupByResultsBlock resultsBlock = (GroupByResultsBlock) operator.nextBlock();
        if (indexedTable == null) {
          indexedTable = createIndexedTable(resultsBlock, 1);
        }
        mergeGroupByResultsBlock(indexedTable, resultsBlock, EXPLAIN_NAME);
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
    }

    boolean setGroupByResult = false;
    while (indexedTable != null && !setGroupByResult) {
      IndexedTable indexedTableToMerge;
      synchronized (this) {
        if (!hasSharedTable()) {
          depositSharedTable(indexedTable);
          setGroupByResult = true;
          continue;
        } else {
          indexedTableToMerge = stealSharedTable();
        }
      }
      if (indexedTable.size() > indexedTableToMerge.size()) {
        indexedTable.merge(indexedTableToMerge);
        indexedTable.absorbTrimStats(indexedTableToMerge);
      } else {
        indexedTableToMerge.merge(indexedTable);
        indexedTableToMerge.absorbTrimStats(indexedTable);
        indexedTable = indexedTableToMerge;
      }
    }
  }
}
