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
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Expands each input row across the grouping sets of a GROUP BY GROUPING SETS / ROLLUP / CUBE query — the
/// multi-stage equivalent of the single-stage per-set row expansion. For every input row and every grouping set it
/// emits one output row in which the columns NOT grouped by that set (the
/// "rolled up" columns) are set to NULL, and appends the synthetic INT discriminator column
/// {@link org.apache.pinot.common.request.context.GroupingSets#GROUPING_ID_COLUMN} whose bit i is set iff union
/// group-by column i is rolled up in that set (matching the single-stage convention so GROUPING() / GROUPING_ID()
/// agree across engines).
///
/// With the rows expanded and tagged, the downstream aggregate is an ordinary GROUP BY over the union group-by
/// columns plus {@code $groupingId} — no grouping-set-specific aggregation logic is needed. This lets grouping sets
/// run over any input (e.g. above a JOIN), not just a leaf table scan.
public class RepeatOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(RepeatOperator.class);
  private static final String EXPLAIN_NAME = "REPEAT";

  private final MultiStageOperator _input;
  private final DataSchema _resultSchema;
  /// Input column index of each union group-by column, in union order; bit i of a grouping-set mask refers to
  /// _unionGroupKeyIds[i].
  private final int[] _unionGroupKeyIds;
  /// Per grouping set: the rolled-up bitmask over the union columns (bit i set iff column i is NOT in the set), in the
  /// order the output rows are emitted. Precomputed from the participation masks.
  private final int[] _groupingIds;
  private final int _inputColumnCount;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  /// @param unionGroupKeyIds input column index of each union group-by column (in union order)
  /// @param groupingSetMasks one participation bitmask per grouping set over the union columns (bit i set iff
  ///                         {@code unionGroupKeyIds[i]} is grouped by that set)
  /// @param resultSchema the input schema with the {@code $groupingId} INT column appended
  public RepeatOperator(OpChainExecutionContext context, MultiStageOperator input, int[] unionGroupKeyIds,
      List<Integer> groupingSetMasks, DataSchema resultSchema) {
    super(context);
    _input = input;
    _resultSchema = resultSchema;
    _unionGroupKeyIds = unionGroupKeyIds;
    _inputColumnCount = resultSchema.size() - 1;
    int fullMask = (1 << unionGroupKeyIds.length) - 1;
    _groupingIds = new int[groupingSetMasks.size()];
    for (int s = 0; s < groupingSetMasks.size(); s++) {
      /// Rolled-up bits are the complement of the participation mask over the union columns.
      _groupingIds[s] = ~groupingSetMasks.get(s) & fullMask;
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
    MseBlock block = _input.nextBlock();
    if (block.isEos()) {
      return block;
    }
    List<Object[]> inputRows = ((MseBlock.Data) block).asRowHeap().getRows();
    int groupingIdIndex = _inputColumnCount;
    List<Object[]> expanded = new ArrayList<>(inputRows.size() * _groupingIds.length);
    for (Object[] inputRow : inputRows) {
      for (int groupingId : _groupingIds) {
        Object[] row = new Object[_inputColumnCount + 1];
        System.arraycopy(inputRow, 0, row, 0, _inputColumnCount);
        /// NULL out the union columns that are rolled up in this set (their bit is set in groupingId).
        for (int i = 0; i < _unionGroupKeyIds.length; i++) {
          if ((groupingId & (1 << i)) != 0) {
            row[_unionGroupKeyIds[i]] = null;
          }
        }
        row[groupingIdIndex] = groupingId;
        expanded.add(row);
      }
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
