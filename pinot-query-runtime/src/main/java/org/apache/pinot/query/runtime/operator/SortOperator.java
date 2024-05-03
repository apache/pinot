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
package org.apache.pinot.query.runtime.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SortOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "SORT";
  private static final Logger LOGGER = LoggerFactory.getLogger(SortOperator.class);

  private final MultiStageOperator _upstreamOperator;
  private final int _fetch;
  private final int _offset;
  private final DataSchema _dataSchema;
  private final PriorityQueue<Object[]> _priorityQueue;
  private final ArrayList<Object[]> _rows;
  private final int _numRowsToKeep;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  private boolean _hasConstructedSortedBlock;
  @Nullable
  private TransferableBlock _eosBlock = null;

  public SortOperator(OpChainExecutionContext context, MultiStageOperator upstreamOperator,
      List<RexExpression> collationKeys, List<RelFieldCollation.Direction> collationDirections,
      List<RelFieldCollation.NullDirection> collationNullDirections, int fetch, int offset, DataSchema dataSchema,
      boolean isInputSorted) {
    this(context, upstreamOperator, collationKeys, collationDirections, collationNullDirections, fetch, offset,
        dataSchema, isInputSorted, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY,
        CommonConstants.Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
  }

  @VisibleForTesting
  SortOperator(OpChainExecutionContext context, MultiStageOperator upstreamOperator,
      List<RexExpression> collationKeys, List<RelFieldCollation.Direction> collationDirections,
      List<RelFieldCollation.NullDirection> collationNullDirections, int fetch, int offset, DataSchema dataSchema,
      boolean isInputSorted, int defaultHolderCapacity, int defaultResponseLimit) {
    super(context);
    _upstreamOperator = upstreamOperator;
    _fetch = fetch;
    _offset = Math.max(offset, 0);
    _dataSchema = dataSchema;
    _hasConstructedSortedBlock = false;
    // Setting numRowsToKeep as default maximum on Broker if limit not set.
    // TODO: make this default behavior configurable.
    _numRowsToKeep = _fetch > 0 ? _fetch + _offset : defaultResponseLimit;
    // Under the following circumstances, the SortOperator is a simple selection with row trim on limit & offset:
    // - There are no collationKeys
    // - 'isInputSorted' is set to true indicating that the data was already sorted
    if (collationKeys.isEmpty() || isInputSorted) {
      _priorityQueue = null;
      _rows = new ArrayList<>(Math.min(defaultHolderCapacity, _numRowsToKeep));
    } else {
      // Use the opposite direction as specified by the collation directions since we need the PriorityQueue to decide
      // which elements to keep and which to remove based on the limits.
      _priorityQueue = new PriorityQueue<>(Math.min(defaultHolderCapacity, _numRowsToKeep),
          new SortUtils.SortComparator(collationKeys, collationDirections, collationNullDirections, dataSchema, true));
      _rows = null;
    }
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public Type getOperatorType() {
    return Type.SORT_OR_LIMIT;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_upstreamOperator);
  }

  @Override
  public void cancel(Throwable e) {
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_hasConstructedSortedBlock) {
      assert _eosBlock != null;
      return _eosBlock;
    }
    TransferableBlock finalBlock = consumeInputBlocks();
    // returning upstream error block if finalBlock contains error.
    if (finalBlock.isErrorBlock()) {
      return finalBlock;
    }
    _statMap.merge(StatKey.REQUIRE_SORT, _priorityQueue != null);
    _eosBlock = updateEosBlock(finalBlock, _statMap);
    return produceSortedBlock();
  }

  private TransferableBlock produceSortedBlock() {
    _hasConstructedSortedBlock = true;
    if (_priorityQueue == null) {
      if (_rows.size() > _offset) {
        List<Object[]> row = _rows.subList(_offset, _rows.size());
        return new TransferableBlock(row, _dataSchema, DataBlock.Type.ROW);
      } else {
        return _eosBlock;
      }
    } else {
      int resultSize = _priorityQueue.size() - _offset;
      if (resultSize <= 0) {
        return _eosBlock;
      }
      Object[][] rowsArr = new Object[resultSize][];
      for (int i = resultSize - 1; i >= 0; i--) {
        Object[] row = _priorityQueue.poll();
        rowsArr[i] = row;
      }
      return new TransferableBlock(Arrays.asList(rowsArr), _dataSchema, DataBlock.Type.ROW);
    }
  }

  private TransferableBlock consumeInputBlocks() {
    TransferableBlock block = _upstreamOperator.nextBlock();
    while (block.isDataBlock()) {
      List<Object[]> container = block.getContainer();
      if (_priorityQueue == null) {
        // TODO: when push-down properly, we shouldn't get more than _numRowsToKeep
        int numRows = _rows.size();
        if (numRows < _numRowsToKeep) {
          if (numRows + container.size() < _numRowsToKeep) {
            _rows.addAll(container);
          } else {
            _rows.addAll(container.subList(0, _numRowsToKeep - numRows));
            if (LOGGER.isDebugEnabled()) {
              // this operatorId is an old name. It is being kept to avoid breaking changes on the log message.
              String operatorId = Joiner.on("_")
                  .join(getClass().getSimpleName(), _context.getStageId(), _context.getServer());
              LOGGER.debug("Early terminate at SortOperator - operatorId={}, opChainId={}", operatorId,
                  _context.getId());
            }
            // setting operator to be early terminated and awaits EOS block next.
            earlyTerminate();
          }
        }
      } else {
        for (Object[] row : container) {
          SelectionOperatorUtils.addToPriorityQueue(row, _priorityQueue, _numRowsToKeep);
        }
      }
      block = _upstreamOperator.nextBlock();
    }
    return block;
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG),
    REQUIRE_SORT(StatMap.Type.BOOLEAN) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    };
    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
