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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
  private static final int MAX_ROWS_PER_OUTPUT_BLOCK = 10_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(SortOperator.class);

  private final MultiStageOperator _input;
  private final DataSchema _dataSchema;
  private final int _offset;
  private final int _numRowsToKeep;
  private final PriorityQueue<Object[]> _priorityQueue;
  private final ArrayList<Object[]> _rows;
  @Nullable
  private final Comparator<Object[]> _fullSortComparator;
  private final boolean _requiresSort;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  private boolean _hasConstructedSortedRows;
  private List<Object[]> _sortedRows = List.of();
  private int _nextOutputRowIndex;
  private MseBlock.Eos _eosBlock;

  public SortOperator(OpChainExecutionContext context, MultiStageOperator input, SortNode node) {
    this(context, input, node, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY,
        CommonConstants.Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT, false);
  }

  /// Creates a sort that retains every input row. This is used for an explicit sender sort whose output is merged by
  /// the receiver, rather than for a user-visible finite top-K.
  public static SortOperator createFullSort(OpChainExecutionContext context, MultiStageOperator input, SortNode node) {
    Preconditions.checkArgument(node.getOffset() <= 0, "Full sender sort cannot have an offset: %s", node.getOffset());
    Preconditions.checkArgument(!node.getCollations().isEmpty(), "Full sender sort requires a collation");
    return new SortOperator(context, input, node, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY,
        CommonConstants.Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT, true);
  }

  @VisibleForTesting
  SortOperator(OpChainExecutionContext context, MultiStageOperator input, SortNode node, int defaultHolderCapacity,
      int defaultResponseLimit) {
    this(context, input, node, defaultHolderCapacity, defaultResponseLimit, false);
  }

  private SortOperator(OpChainExecutionContext context, MultiStageOperator input, SortNode node,
      int defaultHolderCapacity, int defaultResponseLimit, boolean fullSort) {
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
    if (fullSort) {
      _priorityQueue = null;
      _rows = new ArrayList<>(defaultHolderCapacity);
      _fullSortComparator = SortUtils.withTerminationAndUsageSampling(
          new SortUtils.SortComparator(collations, false), EXPLAIN_NAME, _context.getActiveDeadlineMs());
      _requiresSort = true;
    } else if (collations.isEmpty() || isInputSorted(input, collations)) {
      _priorityQueue = null;
      _rows = new ArrayList<>(Math.min(defaultHolderCapacity, _numRowsToKeep));
      _fullSortComparator = null;
      _requiresSort = false;
    } else {
      // Use the opposite direction as specified by the collation directions since we need the PriorityQueue to decide
      // which elements to keep and which to remove based on the limits.
      _priorityQueue = new PriorityQueue<>(Math.min(defaultHolderCapacity, _numRowsToKeep),
          new SortUtils.SortComparator(collations, true));
      _rows = null;
      _fullSortComparator = null;
      _requiresSort = true;
    }
  }

  private static boolean isInputSorted(MultiStageOperator input, List<RelFieldCollation> collations) {
    return input instanceof SortedMultiStageOperator
        && collations.equals(((SortedMultiStageOperator) input).getCollations());
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
  public void cancel(Throwable e) {
    super.cancel(e);
    releaseRetainedRows();
  }

  @Override
  public void close() {
    super.close();
    releaseRetainedRows();
  }

  @Override
  protected void earlyTerminate() {
    super.earlyTerminate();
    releaseRetainedRows();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock getNextBlock() {
    if (!_hasConstructedSortedRows) {
      _eosBlock = consumeInputBlocks();
      // returning upstream error block if finalBlock contains error.
      _statMap.merge(StatKey.REQUIRE_SORT, _requiresSort);
      if (_eosBlock.isError()) {
        return _eosBlock;
      }
      constructSortedRows();
      _hasConstructedSortedRows = true;
    }
    return produceSortedBlock();
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  private void constructSortedRows() {
    if (_fullSortComparator != null) {
      _rows.sort(_fullSortComparator);
      _sortedRows = _rows;
    } else if (_priorityQueue == null) {
      _sortedRows = _rows;
      _nextOutputRowIndex = Math.min(_offset, _rows.size());
      for (int i = 0; i < _nextOutputRowIndex; i++) {
        _sortedRows.set(i, null);
      }
    } else {
      int resultSize = _priorityQueue.size() - _offset;
      if (resultSize <= 0) {
        _priorityQueue.clear();
        return;
      }
      Object[][] rowsArr = new Object[resultSize][];
      for (int i = resultSize - 1; i >= 0; i--) {
        checkTerminationAndSampleUsagePeriodically(resultSize - i - 1, EXPLAIN_NAME);
        Object[] row = _priorityQueue.poll();
        rowsArr[i] = row;
      }
      _priorityQueue.clear();
      _sortedRows = Arrays.asList(rowsArr);
    }
  }

  private MseBlock produceSortedBlock() {
    assert _eosBlock != null;
    int numRows = Math.min(MAX_ROWS_PER_OUTPUT_BLOCK, _sortedRows.size() - _nextOutputRowIndex);
    if (numRows <= 0) {
      releaseSortedRows();
      return _eosBlock;
    }
    int endIndex = _nextOutputRowIndex + numRows;
    List<Object[]> outputRows = new ArrayList<>(numRows);
    for (int i = _nextOutputRowIndex; i < endIndex; i++) {
      outputRows.add(_sortedRows.set(i, null));
    }
    _nextOutputRowIndex = endIndex;
    if (_nextOutputRowIndex == _sortedRows.size()) {
      releaseSortedRows();
    }
    return new RowHeapDataBlock(outputRows, _dataSchema);
  }

  private void releaseSortedRows() {
    releaseRetainedRows();
  }

  private void releaseRetainedRows() {
    if (_sortedRows == _rows) {
      _rows.clear();
    }
    if (_priorityQueue != null) {
      _priorityQueue.clear();
    }
    if (_rows != null) {
      _rows.clear();
    }
    _sortedRows = List.of();
    _nextOutputRowIndex = 0;
  }

  @VisibleForTesting
  int getRetainedRowCount() {
    int retainedRowCount = _sortedRows.size();
    if (_rows != null && _sortedRows != _rows) {
      retainedRowCount += _rows.size();
    }
    if (_priorityQueue != null) {
      retainedRowCount += _priorityQueue.size();
    }
    return retainedRowCount;
  }

  private MseBlock.Eos consumeInputBlocks() {
    MseBlock block = _input.nextBlock();
    while (block.isData()) {
      List<Object[]> container = ((MseBlock.Data) block).asRowHeap().getRows();
      if (_fullSortComparator != null) {
        _rows.addAll(container);
        checkTerminationAndSampleUsage();
      } else if (_priorityQueue == null) {
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
            // Ask only the child to stop. Calling this operator's earlyTerminate() would also clear the retained rows
            // that still need to be returned to the consumer.
            _input.earlyTerminate();
          }
        }
      } else {
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
