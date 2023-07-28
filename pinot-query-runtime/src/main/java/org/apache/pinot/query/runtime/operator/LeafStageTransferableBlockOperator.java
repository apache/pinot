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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * Leaf-stage transfer block operator is used to wrap around the leaf stage process results. They are passed to the
 * Pinot server to execute query thus only one {@link DataTable} were returned. However, to conform with the
 * intermediate stage operators. An additional {@link MetadataBlock} needs to be transferred after the data block.
 *
 * <p>In order to achieve this:
 * <ul>
 *   <li>The leaf-stage result is split into data payload block and metadata payload block.</li>
 *   <li>In case the leaf-stage result contains error or only metadata, we skip the data payload block.</li>
 *   <li>Leaf-stage result blocks are in the {@link DataSchema.ColumnDataType#getStoredColumnDataTypes()} format
 *       thus requires canonicalization.</li>
 * </ul>
 */
public class LeafStageTransferableBlockOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "LEAF_STAGE_TRANSFER_OPERATOR";

  private final LinkedList<ServerQueryRequest> _serverQueryRequestQueue;
  private final DataSchema _desiredDataSchema;
  private final Function<ServerQueryRequest, InstanceResponseBlock> _processCall;

  private InstanceResponseBlock _errorBlock;

  public LeafStageTransferableBlockOperator(OpChainExecutionContext context,
      Function<ServerQueryRequest, InstanceResponseBlock> processCall,
      List<ServerQueryRequest> serverQueryRequestList, DataSchema dataSchema) {
    super(context);
    _processCall = processCall;
    _serverQueryRequestQueue = new LinkedList<>(serverQueryRequestList);
    _desiredDataSchema = dataSchema;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of();
  }

  @Nullable
  @Override
  public String toExplainString() {
      return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_errorBlock != null) {
      throw new RuntimeException("Leaf transfer terminated. next block should no longer be called.");
    }
    // runLeafStage
    InstanceResponseBlock responseBlock = getNextBlockFromLeafStage();
    if (responseBlock == null) {
      // finished getting next block from leaf stage. returning EOS
      return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock());
    } else if (!responseBlock.getExceptions().isEmpty()) {
      // get error from leaf stage, return ERROR
      _errorBlock = responseBlock;
      return new TransferableBlock(DataBlockUtils.getErrorDataBlock(_errorBlock.getExceptions()));
    } else {
      // return normal block.
      OperatorStats operatorStats = _opChainStats.getOperatorStats(_context, getOperatorId());
      operatorStats.recordExecutionStats(responseBlock.getResponseMetadata());
      if (responseBlock.getResultsBlock() != null && responseBlock.getResultsBlock().getNumRows() > 0) {
        return composeTransferableBlock(responseBlock, _desiredDataSchema);
      } else {
        return new TransferableBlock(Collections.emptyList(), _desiredDataSchema, DataBlock.Type.ROW);
      }
    }
  }

  private @Nullable InstanceResponseBlock getNextBlockFromLeafStage() {
    if (!_serverQueryRequestQueue.isEmpty()) {
      ServerQueryRequest request = _serverQueryRequestQueue.pop();
      return _processCall.apply(request);
    } else {
      return null;
    }
  }

  /**
   * Leaf stage operators should always collect stats for the tables used in queries
   * Otherwise the Broker response will just contain zeros for every stat value
   */
  @Override
  protected boolean shouldCollectStats() {
    return true;
  }

  /**
   * this is data transfer block compose method is here to ensure that V1 results match what the expected projection
   * schema in the calcite logical operator.
   * <p> it applies different clean up mechanism based on different type of {@link BaseResultsBlock} and the
   *     {@link org.apache.pinot.core.query.request.context.QueryContext}.</p>
   * <p> this also applies to the canonicalization of the data types during post post-process step.</p>
   *
   * @param responseBlock result block from leaf stage
   * @param desiredDataSchema the desired schema for send operator
   * @return the converted {@link TransferableBlock} that conform with the desiredDataSchema
   */
  private static TransferableBlock composeTransferableBlock(InstanceResponseBlock responseBlock,
      DataSchema desiredDataSchema) {
    BaseResultsBlock resultsBlock = responseBlock.getResultsBlock();
    if (resultsBlock instanceof SelectionResultsBlock) {
      return composeSelectTransferableBlock(responseBlock, desiredDataSchema);
    } else if (resultsBlock instanceof AggregationResultsBlock) {
      return composeAggregationTransferableBlock(responseBlock, desiredDataSchema);
    } else if (resultsBlock instanceof GroupByResultsBlock) {
      return composeGroupByTransferableBlock(responseBlock, desiredDataSchema);
    } else if (resultsBlock instanceof DistinctResultsBlock) {
      return composeDistinctTransferableBlock(responseBlock, desiredDataSchema);
    } else {
      throw new IllegalArgumentException("Unsupported result block type: " + resultsBlock);
    }
  }

  /**
   * we only need to rearrange columns when distinct is not conforming with selection columns, specifically:
   * <ul>
   *   <li> when distinct is not returning final result:
   *       it should never happen as non-final result contains Object opaque columns v2 engine can't process.</li>
   *   <li> when distinct columns are not all being selected:
   *       it should never happen as leaf stage MUST return the entire list.</li>
   * </ul>
   *
   * @see LeafStageTransferableBlockOperator#composeTransferableBlock(InstanceResponseBlock, DataSchema).
   */
  @SuppressWarnings("ConstantConditions")
  private static TransferableBlock composeDistinctTransferableBlock(InstanceResponseBlock responseBlock,
      DataSchema desiredDataSchema) {
    DataSchema resultSchema = responseBlock.getDataSchema();
    Preconditions.checkState(isDataSchemaColumnTypesCompatible(desiredDataSchema.getColumnDataTypes(),
        resultSchema.getColumnDataTypes()), "Incompatible selection result data schema: "
        + " Expected: " + desiredDataSchema + ". Actual: " + resultSchema);
    return composeDirectTransferableBlock(responseBlock, desiredDataSchema);
  }

  /**
   * Calcite generated {@link DataSchema} should conform with Pinot's group by result schema thus we only need to check
   * for correctness similar to distinct case.
   *
   * @see LeafStageTransferableBlockOperator#composeDirectTransferableBlock(InstanceResponseBlock, DataSchema).
   * @see LeafStageTransferableBlockOperator#composeTransferableBlock(InstanceResponseBlock, DataSchema).
   */
  @SuppressWarnings("ConstantConditions")
  private static TransferableBlock composeGroupByTransferableBlock(InstanceResponseBlock responseBlock,
      DataSchema desiredDataSchema) {
    DataSchema resultSchema = responseBlock.getDataSchema();
    Preconditions.checkState(isDataSchemaColumnTypesCompatible(desiredDataSchema.getColumnDataTypes(),
        resultSchema.getColumnDataTypes()), "Incompatible selection result data schema: "
        + " Expected: " + desiredDataSchema + ". Actual: " + resultSchema);
    return composeDirectTransferableBlock(responseBlock, desiredDataSchema);
  }


  /**
   * Calcite generated {@link DataSchema} should conform with Pinot's agg result schema thus we only need to check
   * for correctness similar to distinct case.
   *
   * @see LeafStageTransferableBlockOperator#composeDirectTransferableBlock(InstanceResponseBlock, DataSchema).
   * @see LeafStageTransferableBlockOperator#composeTransferableBlock(InstanceResponseBlock, DataSchema).
   */
  @SuppressWarnings("ConstantConditions")
  private static TransferableBlock composeAggregationTransferableBlock(InstanceResponseBlock responseBlock,
      DataSchema desiredDataSchema) {
    DataSchema resultSchema = responseBlock.getDataSchema();
    Preconditions.checkState(isDataSchemaColumnTypesCompatible(desiredDataSchema.getColumnDataTypes(),
        resultSchema.getColumnDataTypes()), "Incompatible selection result data schema: "
        + " Expected: " + desiredDataSchema + ". Actual: " + resultSchema);
    return composeDirectTransferableBlock(responseBlock, desiredDataSchema);
  }

  /**
   * Only re-arrange columns to match the projection in the case of select / order-by, when the desiredDataSchema
   * doesn't conform with the result block schema exactly.
   *
   * @see LeafStageTransferableBlockOperator#composeTransferableBlock(InstanceResponseBlock, DataSchema).
   */
  @SuppressWarnings("ConstantConditions")
  private static TransferableBlock composeSelectTransferableBlock(InstanceResponseBlock responseBlock,
      DataSchema desiredDataSchema) {
    DataSchema resultSchema = responseBlock.getDataSchema();
    List<String> selectionColumns = SelectionOperatorUtils.getSelectionColumns(responseBlock.getQueryContext(),
        resultSchema);
    int[] columnIndices = SelectionOperatorUtils.getColumnIndices(selectionColumns, resultSchema);
    if (!inOrder(columnIndices)) {
      DataSchema adjustedResultSchema = SelectionOperatorUtils.getSchemaForProjection(resultSchema, columnIndices);
      Preconditions.checkState(isDataSchemaColumnTypesCompatible(desiredDataSchema.getColumnDataTypes(),
          adjustedResultSchema.getColumnDataTypes()), "Incompatible selection result data schema: "
          + " Expected: " + desiredDataSchema + ". Actual: " + adjustedResultSchema
          + " Column Ordering: " + Arrays.toString(columnIndices));
      return composeColumnIndexedTransferableBlock(responseBlock, adjustedResultSchema, columnIndices);
    } else {
      return composeDirectTransferableBlock(responseBlock, desiredDataSchema);
    }
  }

  /**
   * Created {@link TransferableBlock} using column indices.
   *
   * @see LeafStageTransferableBlockOperator#composeTransferableBlock(InstanceResponseBlock, DataSchema).
   */
  private static TransferableBlock composeColumnIndexedTransferableBlock(InstanceResponseBlock responseBlock,
      DataSchema desiredDataSchema, int[] columnIndices) {
    Collection<Object[]> resultRows = responseBlock.getRows();
    List<Object[]> extractedRows = new ArrayList<>(resultRows.size());
    if (resultRows instanceof List) {
      for (Object[] row : resultRows) {
        extractedRows.add(TypeUtils.canonicalizeRow(row, desiredDataSchema, columnIndices));
      }
    } else if (resultRows instanceof PriorityQueue) {
      PriorityQueue<Object[]> priorityQueue = (PriorityQueue<Object[]>) resultRows;
      while (!priorityQueue.isEmpty()) {
        extractedRows.add(TypeUtils.canonicalizeRow(priorityQueue.poll(), desiredDataSchema, columnIndices));
      }
    }
    return new TransferableBlock(extractedRows, desiredDataSchema, DataBlock.Type.ROW);
  }

  /**
   * Fallback mechanism for {@link TransferableBlock}, used when no special handling is necessary. This method only
   * performs {@link DataSchema.ColumnDataType} canonicalization.
   *
   * @see LeafStageTransferableBlockOperator#composeTransferableBlock(InstanceResponseBlock, DataSchema).
   */
  private static TransferableBlock composeDirectTransferableBlock(InstanceResponseBlock responseBlock,
      DataSchema desiredDataSchema) {
    Collection<Object[]> resultRows = responseBlock.getRows();
    List<Object[]> extractedRows = new ArrayList<>(resultRows.size());
    if (resultRows instanceof List) {
      for (Object[] orgRow : resultRows) {
        extractedRows.add(TypeUtils.canonicalizeRow(orgRow, desiredDataSchema));
      }
    } else if (resultRows instanceof PriorityQueue) {
      PriorityQueue<Object[]> priorityQueue = (PriorityQueue<Object[]>) resultRows;
      while (!priorityQueue.isEmpty()) {
        extractedRows.add(TypeUtils.canonicalizeRow(priorityQueue.poll(), desiredDataSchema));
      }
    } else {
      throw new UnsupportedOperationException("Unsupported collection type: " + resultRows.getClass());
    }
    return new TransferableBlock(extractedRows, desiredDataSchema, DataBlock.Type.ROW);
  }

  private static boolean inOrder(int[] columnIndices) {
    for (int i = 0; i < columnIndices.length; i++) {
      if (columnIndices[i] != i) {
        return false;
      }
    }
    return true;
  }

  private static boolean isDataSchemaColumnTypesCompatible(DataSchema.ColumnDataType[] desiredTypes,
      DataSchema.ColumnDataType[] givenTypes) {
    if (desiredTypes.length != givenTypes.length) {
      return false;
    }
    for (int i = 0; i < desiredTypes.length; i++) {
      if (desiredTypes[i] != givenTypes[i] && !givenTypes[i].isSuperTypeOf(desiredTypes[i])) {
        return false;
      }
    }
    return true;
  }
}
