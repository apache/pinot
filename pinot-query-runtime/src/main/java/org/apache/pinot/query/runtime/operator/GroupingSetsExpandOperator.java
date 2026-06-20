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
import org.apache.pinot.query.planner.plannode.GroupingSetsExpandNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Native multi-stage-engine expansion for {@code GROUP BY GROUPING SETS / ROLLUP / CUBE}. For each input row this
 * operator emits one output row per grouping set: it copies the input row, sets the union group-by columns that are
 * rolled up in that set to {@code NULL}, and writes the per-set discriminator into the trailing {@code $groupingId}
 * column (the last column of the output schema). See {@link GroupingSetsExpandNode} for the discriminator convention
 * (bit {@code p} set iff {@code groupingColumns[p]} is rolled up). A downstream ordinary aggregate keyed on
 * {@code [unionGroupKeys..., $groupingId]} then computes every grouping set in a single pass while keeping genuine
 * NULL groups distinct from rolled-up NULL groups.
 */
public class GroupingSetsExpandOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupingSetsExpandOperator.class);
  private static final String EXPLAIN_NAME = "GROUPING_SETS_EXPAND";

  private final MultiStageOperator _input;
  private final DataSchema _resultSchema;
  private final int[] _groupingColumns;
  private final int[] _groupingIds;
  // Index of the synthetic $groupingId column in the output row, which equals the number of input columns.
  private final int _groupingIdIndex;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public GroupingSetsExpandOperator(OpChainExecutionContext context, MultiStageOperator input,
      GroupingSetsExpandNode node) {
    super(context);
    _input = input;
    _resultSchema = node.getDataSchema();
    _groupingColumns = node.getGroupingColumns().stream().mapToInt(Integer::intValue).toArray();
    _groupingIds = node.getGroupingIds().stream().mapToInt(Integer::intValue).toArray();
    _groupingIdIndex = _resultSchema.size() - 1;
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
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
  public Type getOperatorType() {
    return Type.GROUPING_SETS_EXPAND;
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
    MseBlock.Data dataBlock = (MseBlock.Data) block;
    List<Object[]> inputRows = dataBlock.asRowHeap().getRows();
    List<Object[]> outRows = new ArrayList<>(inputRows.size() * _groupingIds.length);
    for (Object[] row : inputRows) {
      for (int groupingId : _groupingIds) {
        Object[] out = new Object[_resultSchema.size()];
        System.arraycopy(row, 0, out, 0, row.length);
        // Null out every union group-by column that is rolled up (bit set) in this grouping set.
        for (int p = 0; p < _groupingColumns.length; p++) {
          if (((groupingId >> p) & 1) == 1) {
            out[_groupingColumns[p]] = null;
          }
        }
        out[_groupingIdIndex] = groupingId;
        outRows.add(out);
      }
    }
    return new RowHeapDataBlock(outRows, _resultSchema);
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
    EMITTED_ROWS(StatMap.Type.LONG),
    /**
     * Allocated memory in bytes for this operator or its children in the same stage.
     */
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /**
     * Time spent on GC while this operator or its children in the same stage were running.
     */
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
