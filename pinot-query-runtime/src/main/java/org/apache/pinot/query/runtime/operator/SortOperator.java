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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SortOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "SORT";
  private static final Logger LOGGER = LoggerFactory.getLogger(SortOperator.class);

  private final MultiStageOperator _input;
  private final DataSchema _dataSchema;
  private final int _offset;
  private final int _numRowsToKeep;
  /// Whether this operator has to sort, or is a plain limit/offset over an already-sorted input. Decides which of
  /// the two buffers below is in use. Kept separate from them so that releasing a buffer cannot change the mode.
  private final boolean _requiresSort;
  /// The rows-to-keep heap, used when [#_requiresSort]. Never handed downstream — [#produceSortedBlock()] drains it
  /// into a fresh array — so releasing replaces it outright rather than clearing it, which would keep the backing
  /// array alive.
  @Nullable
  private PriorityQueue<Object[]> _priorityQueue;
  /// The buffered rows, used when not [#_requiresSort]. Handed downstream as a sublist view of itself, so releasing
  /// must drop the reference rather than empty the list.
  @Nullable
  private ArrayList<Object[]> _rows;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  private boolean _hasConstructedSortedBlock;
  private MseBlock.Eos _eosBlock;

  public SortOperator(OpChainExecutionContext context, MultiStageOperator input, SortNode node) {
    this(context, input, node, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY,
        CommonConstants.Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
  }

  @VisibleForTesting
  SortOperator(OpChainExecutionContext context, MultiStageOperator input, SortNode node, int defaultHolderCapacity,
      int defaultResponseLimit) {
    super(context);
    _input = input;
    _dataSchema = node.getDataSchema();
    _offset = Math.max(node.getOffset(), 0);
    // Setting numRowsToKeep as default maximum on Broker if limit not set.
    // TODO: make this default behavior configurable.
    int fetch = node.getFetch();
    _numRowsToKeep = fetch > 0 ? fetch + _offset : defaultResponseLimit;
    // Under the following circumstances, the SortOperator is a simple selection with row trim on limit & offset:
    // - There is no collation
    // - Input is already sorted
    List<RelFieldCollation> collations = node.getCollations();
    _requiresSort = !(collations.isEmpty() || input instanceof SortedMailboxReceiveOperator);
    if (!_requiresSort) {
      _priorityQueue = null;
      _rows = new ArrayList<>(Math.min(defaultHolderCapacity, _numRowsToKeep));
    } else {
      // Use the opposite direction as specified by the collation directions since we need the PriorityQueue to decide
      // which elements to keep and which to remove based on the limits.
      _priorityQueue = new PriorityQueue<>(Math.min(defaultHolderCapacity, _numRowsToKeep),
          new SortUtils.SortComparator(collations, true));
      _rows = null;
    }
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
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
    return List.of(_input);
  }

  @Override
  protected void releaseBuffers() {
    _priorityQueue = null;
    _rows = null;
  }

  @Override
  protected boolean hasBufferedState() {
    return _priorityQueue != null || _rows != null;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_hasConstructedSortedBlock) {
      assert _eosBlock != null;
      return _eosBlock;
    }
    _eosBlock = consumeInputBlocks();
    // returning upstream error block if finalBlock contains error.
    _statMap.merge(StatKey.REQUIRE_SORT, _requiresSort);
    if (_eosBlock.isError()) {
      return _eosBlock;
    }
    return produceSortedBlock();
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  private MseBlock produceSortedBlock() {
    _hasConstructedSortedBlock = true;
    if (!_requiresSort) {
      assert _rows != null : "Rows should not be null when producing the sorted block";
      if (_rows.size() > _offset) {
        List<Object[]> row = _rows.subList(_offset, _rows.size());
        return new RowHeapDataBlock(row, _dataSchema);
      } else {
        return _eosBlock;
      }
    } else {
      assert _priorityQueue != null : "Priority queue should not be null when producing the sorted block";
      int resultSize = _priorityQueue.size() - _offset;
      if (resultSize <= 0) {
        return _eosBlock;
      }
      Object[][] rowsArr = new Object[resultSize][];
      for (int i = resultSize - 1; i >= 0; i--) {
        Object[] row = _priorityQueue.poll();
        rowsArr[i] = row;
      }
      return new RowHeapDataBlock(Arrays.asList(rowsArr), _dataSchema);
    }
  }

  private MseBlock.Eos consumeInputBlocks() {
    MseBlock block = _input.nextBlock();
    while (block.isData()) {
      List<Object[]> container = ((MseBlock.Data) block).asRowHeap().getRows();
      if (!_requiresSort) {
        assert _rows != null : "Rows should not be null when consuming input blocks";
        // TODO: when push-down properly, we shouldn't get more than _numRowsToKeep
        int numRows = _rows.size();
        if (numRows < _numRowsToKeep) {
          if (numRows + container.size() < _numRowsToKeep) {
            _rows.addAll(container);
          } else {
            _rows.addAll(container.subList(0, _numRowsToKeep - numRows));
            if (LOGGER.isDebugEnabled()) {
              // this operatorId is an old name. It is being kept to avoid breaking changes on the log message.
              String operatorId =
                  Joiner.on("_").join(getClass().getSimpleName(), _context.getStageId(), _context.getServer());
              LOGGER.debug("Early terminate at SortOperator - operatorId={}, opChainId={}", operatorId,
                  _context.getId());
            }
            // setting operator to be early terminated and awaits EOS block next.
            earlyTerminate();
          }
        }
      } else {
        assert _priorityQueue != null : "Priority queue should not be null when consuming input blocks";
        for (Object[] row : container) {
          SelectionOperatorUtils.addToPriorityQueue(row, _priorityQueue, _numRowsToKeep);
        }
        checkTerminationAndSampleUsage();
      }
      block = _input.nextBlock();
    }
    return (MseBlock.Eos) block;
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG), REQUIRE_SORT(StatMap.Type.BOOLEAN) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    /// Allocated memory in bytes for this operator or its children in the same stage.
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /// Time spent on GC while this operator or its children in the same stage were running.
    GC_TIME_MS(StatMap.Type.LONG);

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
