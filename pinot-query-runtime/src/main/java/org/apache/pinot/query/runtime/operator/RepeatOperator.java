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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Expands each input row across the grouping sets of a GROUP BY GROUPING SETS / ROLLUP / CUBE query — the
/// multi-stage equivalent of the single-stage per-set row expansion. For every input row and every grouping set it
/// emits one output row of the shape {@code [input columns..., group-key copies..., $groupingId]}: the original
/// input columns are carried through UNTOUCHED (aggregation arguments may reference a grouping column, so nulling
/// in place would corrupt them — e.g. SUM(b) under ROLLUP(a, b) must still aggregate the real b values for the
/// (a) subtotal), one appended copy per union group-by column holds the group-key value — NULL when the column is
/// rolled up (not grouped by) in the set — and the trailing synthetic INT discriminator column
/// {@link org.apache.pinot.common.request.context.GroupingSets#GROUPING_ID_COLUMN} carries the grouping set's
/// ordinal (its index in the plan's grouping-set list, matching the single-stage convention so GROUPING() /
/// GROUPING_ID() agree across engines). Because the ordinal is not a per-column bitmask, the number of grouping
/// columns is unlimited. The downstream aggregate groups by the appended copies plus the ordinal.
///
/// With the rows expanded and tagged, the downstream aggregate is an ordinary GROUP BY over the appended key
/// copies plus {@code $groupingId} — no grouping-set-specific aggregation logic is needed. This lets grouping sets
/// run over any input (e.g. above a JOIN), not just a leaf table scan.
///
/// The expansion is streamed one grouping set at a time: each {@link #getNextBlock()} call emits the current input
/// block expanded for a single set, so the transient materialization is bounded by the input block size rather than
/// multiplied by the set count. Not thread-safe: like all multi-stage operators, it executes on the single OpChain
/// thread.
public class RepeatOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(RepeatOperator.class);
  private static final String EXPLAIN_NAME = "REPEAT";

  private final MultiStageOperator _input;
  private final DataSchema _resultSchema;
  /// Input column index of each union group-by column, in union order; output column _inputColumnCount + i is the
  /// group-key copy of _unionGroupKeyIds[i].
  private final int[] _unionGroupKeyIds;
  /// Per grouping set (in ordinal order): membership over the union columns (_setContains[s][i] iff union column i
  /// participates in set s; its key copy is NULLed otherwise). Output rows of set s carry the ordinal s in the
  /// discriminator column.
  private final boolean[][] _setContains;
  private final int _numSets;
  private final int _inputColumnCount;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  /// Rows of the input block currently being expanded, or null when the next input block must be pulled.
  @Nullable
  private List<Object[]> _currentRows;
  /// The grouping set (ordinal) the next getNextBlock() call will expand the current input block for.
  private int _currentSet;

  /// @param unionGroupKeyIds input column index of each union group-by column (in union order)
  /// @param groupingSets per grouping set (in ordinal order), the union-column indexes participating in
  ///                     (grouped by) that set — {@code unionGroupKeyIds[i]} for member index i
  /// @param resultSchema the input schema with one group-key copy column per union group-by column and the
  ///                     {@code $groupingId} INT column appended
  public RepeatOperator(OpChainExecutionContext context, MultiStageOperator input, int[] unionGroupKeyIds,
      List<List<Integer>> groupingSets, DataSchema resultSchema) {
    super(context);
    _input = input;
    _resultSchema = resultSchema;
    _unionGroupKeyIds = unionGroupKeyIds;
    _inputColumnCount = resultSchema.size() - unionGroupKeyIds.length - 1;
    _numSets = groupingSets.size();
    _setContains = new boolean[_numSets][unionGroupKeyIds.length];
    for (int s = 0; s < _numSets; s++) {
      for (int memberIndex : groupingSets.get(s)) {
        _setContains[s][memberIndex] = true;
      }
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
    return Type.REPEAT;
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
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_currentRows == null) {
      MseBlock block = _input.nextBlock();
      if (block.isEos()) {
        return block;
      }
      _currentRows = ((MseBlock.Data) block).asRowHeap().getRows();
      _currentSet = 0;
    }
    /// Expand the current input block for one grouping set per call.
    int set = _currentSet;
    boolean[] contains = _setContains[set];
    int[] unionGroupKeyIds = _unionGroupKeyIds;
    int numUnionKeys = unionGroupKeyIds.length;
    int inputColumnCount = _inputColumnCount;
    /// The ordinal is constant across the block; box it once instead of once per output row.
    Object groupingId = set;
    List<Object[]> expanded = new ArrayList<>(_currentRows.size());
    for (Object[] inputRow : _currentRows) {
      Object[] row = new Object[inputColumnCount + numUnionKeys + 1];
      System.arraycopy(inputRow, 0, row, 0, inputColumnCount);
      /// Append the group-key copies: the union column's value where it participates in this set, NULL where it
      /// is rolled up. The original input columns stay untouched for aggregation arguments.
      for (int i = 0; i < numUnionKeys; i++) {
        row[inputColumnCount + i] = contains[i] ? inputRow[unionGroupKeyIds[i]] : null;
      }
      row[inputColumnCount + numUnionKeys] = groupingId;
      expanded.add(row);
      /// Honor query cancellation/deadline and sample resource usage during the row amplification, like the
      /// other row-amplifying operators (joins, window).
      checkTerminationAndSampleUsagePeriodically(expanded.size(), EXPLAIN_NAME);
    }
    if (++_currentSet == _numSets) {
      _currentRows = null;
    }
    return new RowHeapDataBlock(expanded, _resultSchema);
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
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
