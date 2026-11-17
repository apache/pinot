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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.docvalsets.DataBlockValSet;
import org.apache.pinot.core.operator.docvalsets.FilteredDataBlockValSet;
import org.apache.pinot.core.operator.docvalsets.FilteredRowBasedBlockValSet;
import org.apache.pinot.core.operator.docvalsets.RowBasedBlockValSet;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// AggregateOperator is used to aggregate values over a (potentially empty) set of group by keys in V2/MSQE.
/// Output data will be in the format of \[group by key, aggregate result1, ... aggregate resultN\]
/// When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
public class AggregateOperator extends BaseAggregateOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateOperator.class);
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";

  /// Whether this operator groups. Decides which of the two executors is in use; kept separate from them so that
  /// releasing an executor cannot change the mode.
  private final boolean _isGroupBy;
  @Nullable
  private MultistageAggregationExecutor _aggregationExecutor;

  public AggregateOperator(OpChainExecutionContext context, MultiStageOperator input, AggregateNode node) {
    super(context, input, node.getDataSchema(), node.getAggCalls(), node.getFilterArgs(), node.getNodeHint(),
        node.getLimit(), node.getCollations());

    /// Grouping-set aggregates never reach this operator directly: PlanNodeToOpChain pre-wraps the input in a
    /// RepeatOperator and rewrites the node into the equivalent plain GROUP BY over the expanded input.
    List<Integer> groupKeys = node.getGroupKeys();

    // Initialize the appropriate executor.
    AggregateNode.AggType aggType = node.getAggType();
    boolean leafReturnFinalResult = node.isLeafReturnFinalResult();
    _isGroupBy = !groupKeys.isEmpty();
    if (!_isGroupBy) {
      _aggregationExecutor =
          new MultistageAggregationExecutor(_aggFunctions, _filterArgIds, _maxFilterArgId, aggType,
              leafReturnFinalResult, _resultSchema);
      _groupByExecutor = null;
    } else {
      _groupByExecutor =
          new MultistageGroupByExecutor(getGroupKeyIds(groupKeys), _aggFunctions, _filterArgIds, _maxFilterArgId,
              aggType, leafReturnFinalResult, _resultSchema, context.getOpChainMetadata(), node.getNodeHint());
      _aggregationExecutor = null;
    }
  }

  @Override
  public Type getOperatorType() {
    return Type.AGGREGATE;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /// Drops the executors, and with them the group-by hash maps and the aggregate result holders they own. Marks the
  /// operator finished at the same time, so that a later [#getNextBlock()] returns the cached end of stream instead
  /// of trying to consume the input again with a dropped executor.
  @Override
  protected void releaseBuffers() {
    _aggregationExecutor = null;
    super.releaseBuffers();
  }

  @Override
  protected boolean hasBufferedState() {
    return _aggregationExecutor != null || super.hasBufferedState();
  }

  @Override
  protected MseBlock.Eos consumeInput() {
    return _isGroupBy ? consumeGroupBy() : consumeAggregation();
  }

  @Override
  protected MseBlock produceAggregatedBlock() {
    if (!_isGroupBy) {
      assert _aggregationExecutor != null;
      List<Object[]> rows = _aggregationExecutor.getResult();
      recordApproximatedResult();
      return new RowHeapDataBlock(rows, _resultSchema, _aggFunctions);
    } else {
      return produceGroupByBlock();
    }
  }

  /// Consumes the input blocks as an aggregation
  ///
  /// @return the last block, which must always be either an error or the end of the stream
  private MseBlock.Eos consumeAggregation() {
    assert _aggregationExecutor != null;
    MseBlock block = _input.nextBlock();
    while (block.isData()) {
      _aggregationExecutor.processBlock((MseBlock.Data) block);
      checkTerminationAndSampleUsage();
      block = _input.nextBlock();
    }
    return (MseBlock.Eos) block;
  }

  static RoaringBitmap getMatchedBitmap(MseBlock.Data block, int filterArgId) {
    Preconditions.checkArgument(filterArgId >= 0, "Got negative filter argument id: %s", filterArgId);
    RoaringBitmap matchedBitmap = new RoaringBitmap();
    if (block.isRowHeap()) {
      List<Object[]> rows = block.asRowHeap().getRows();
      int numRows = rows.size();
      for (int rowId = 0; rowId < numRows; rowId++) {
        // Treat NULL filter values as non-matching (SQL WHERE clause semantics).
        Object filterValue = rows.get(rowId)[filterArgId];
        if (filterValue != null && (int) filterValue == 1) {
          matchedBitmap.add(rowId);
        }
      }
    } else {
      DataBlock dataBlock = block.asSerialized().getDataBlock();
      int numRows = dataBlock.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (dataBlock.getInt(rowId, filterArgId) == 1) {
          matchedBitmap.add(rowId);
        }
      }
    }
    return matchedBitmap;
  }

  static Map<ExpressionContext, BlockValSet> getBlockValSetMap(AggregationFunction<?, ?> aggFunction,
      MseBlock.Data block) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Map.of();
    }
    DataSchema dataSchema = block.getDataSchema();
    assert dataSchema != null;
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    if (block.isRowHeap()) {
      List<Object[]> rows = block.asRowHeap().getRows();
      for (ExpressionContext expression : expressions) {
        String identifier = expression.getIdentifier();
        if (identifier != null) {
          int colId = fromIdentifierToColId(identifier);
          blockValSetMap.put(expression,
              new RowBasedBlockValSet(dataSchema.getColumnDataType(colId), rows, colId, true));
        }
      }
    } else {
      DataBlock dataBlock = block.asSerialized().getDataBlock();
      for (ExpressionContext expression : expressions) {
        String identifier = expression.getIdentifier();
        if (identifier != null) {
          int colId = fromIdentifierToColId(identifier);
          blockValSetMap.put(expression, new DataBlockValSet(dataSchema.getColumnDataType(colId), dataBlock, colId));
        }
      }
    }
    return blockValSetMap;
  }

  static Map<ExpressionContext, BlockValSet> getFilteredBlockValSetMap(AggregationFunction<?, ?> aggFunction,
      MseBlock.Data block, int numMatchedRows, RoaringBitmap matchedBitmap) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Map.of();
    }
    DataSchema dataSchema = block.getDataSchema();
    assert dataSchema != null;
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    if (block.isRowHeap()) {
      List<Object[]> rows = block.asRowHeap().getRows();
      for (ExpressionContext expression : expressions) {
        String identifier = expression.getIdentifier();
        if (identifier != null) {
          int colId = fromIdentifierToColId(identifier);
          blockValSetMap.put(expression,
              new FilteredRowBasedBlockValSet(dataSchema.getColumnDataType(colId), rows, colId, numMatchedRows,
                  matchedBitmap, true));
        }
      }
    } else {
      DataBlock dataBlock = block.asSerialized().getDataBlock();
      for (ExpressionContext expression : expressions) {
        String identifier = expression.getIdentifier();
        if (identifier != null) {
          int colId = fromIdentifierToColId(identifier);
          blockValSetMap.put(expression,
              new FilteredDataBlockValSet(block.getDataSchema().getColumnDataType(colId), dataBlock, colId,
                  numMatchedRows, matchedBitmap));
        }
      }
    }
    return blockValSetMap;
  }

  /// Returns the id of the column that holds the intermediate results of `aggFunction` in a merge block.
  static int getIntermediateResultColId(AggregationFunction<?, ?> aggFunction) {
    ExpressionContext firstArgument = aggFunction.getInputExpressions().get(0);
    Preconditions.checkState(firstArgument.getType() == ExpressionContext.Type.IDENTIFIER,
        "Expected the first argument to be IDENTIFIER, got: %s", firstArgument.getType());
    return fromIdentifierToColId(firstArgument.getIdentifier());
  }

  /// Returns the serialized data block of `block` when the intermediate results of `aggFunction` are serialized
  /// custom objects in it, or `null` otherwise. Merging such a block with
  /// `AggregationFunctionUtils.mergeSerialized` skips deserializing each intermediate result into a separate object.
  @Nullable
  static DataBlock getSerializedIntermediateResults(AggregationFunction<?, ?> aggFunction, MseBlock.Data block) {
    if (block.isRowHeap()) {
      return null;
    }
    DataBlock dataBlock = block.asSerialized().getDataBlock();
    ColumnDataType storedType =
        dataBlock.getDataSchema().getColumnDataType(getIntermediateResultColId(aggFunction)).getStoredType();
    return storedType == ColumnDataType.OBJECT ? dataBlock : null;
  }

  static Object[] getIntermediateResults(AggregationFunction<?, ?> aggFunction, MseBlock.Data block) {
    int colId = getIntermediateResultColId(aggFunction);
    int numRows = block.getNumRows();
    if (block.isRowHeap()) {
      Object[] values = new Object[numRows];
      List<Object[]> rows = block.asRowHeap().getRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] = rows.get(rowId)[colId];
      }
      return values;
    } else {
      return DataBlockExtractUtils.extractAggResult(block.asSerialized().getDataBlock(), colId, aggFunction);
    }
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
    GROUPS_TRIMMED(StatMap.Type.BOOLEAN),
    NUM_GROUPS_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_GROUPS_WARNING_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_GROUPS(StatMap.Type.LONG) {
      @Override
      public long merge(long value1, long value2) {
        return Math.max(value1, value2);
      }
    },
    /// Allocated memory in bytes for this operator or its children in the same stage.
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /// Time spent on GC while this operator or its children in the same stage were running.
    GC_TIME_MS(StatMap.Type.LONG),
    /// Whether a function that can answer exactly returned an approximate final result, for example a SMART function
    /// that switched to a sketch.
    APPROXIMATED_RESULT(StatMap.Type.BOOLEAN);

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
