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

import com.clearspring.analytics.util.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


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
public class LeafStageTransferableBlockOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "LEAF_STAGE_TRANSFER_OPERATOR";

  private final InstanceResponseBlock _errorBlock;
  private final List<InstanceResponseBlock> _baseResultBlock;
  private final DataSchema _desiredDataSchema;
  private int _currentIndex;

  public LeafStageTransferableBlockOperator(List<InstanceResponseBlock> baseResultBlock, DataSchema dataSchema) {
    _baseResultBlock = baseResultBlock;
    _desiredDataSchema = dataSchema;
    _errorBlock = baseResultBlock.stream().filter(e -> !e.getExceptions().isEmpty()).findFirst().orElse(null);
    _currentIndex = 0;
  }

  @Override
  public List<Operator> getChildOperators() {
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_currentIndex < 0) {
      throw new RuntimeException("Leaf transfer terminated. next block should no longer be called.");
    }
    if (_errorBlock != null) {
      _currentIndex = -1;
      return new TransferableBlock(DataBlockUtils.getErrorDataBlock(_errorBlock.getExceptions()));
    } else {
      if (_currentIndex < _baseResultBlock.size()) {
        InstanceResponseBlock responseBlock = _baseResultBlock.get(_currentIndex++);
        if (responseBlock.getResultsBlock() != null) {
          DataSchema dataSchema = responseBlock.getDataSchema();
          boolean requiresCleanup = responseBlock.getResultsBlock() instanceof SelectionResultsBlock
              && dataSchema != null && responseBlock.getRows() != null && !responseBlock.getRows().isEmpty();

          // 1. canonicalize data schema
          dataSchema = requiresCleanup ? cleanUpDataSchema(responseBlock, _desiredDataSchema) : dataSchema;
          // 2. canonicalize data block
          List<Object[]> rows = cleanUpDataBlock(responseBlock, dataSchema, requiresCleanup);
          return new TransferableBlock(rows, dataSchema, DataBlock.Type.ROW);
        } else {
          return new TransferableBlock(Collections.emptyList(), responseBlock.getDataSchema(), DataBlock.Type.ROW);
        }
      } else {
        _currentIndex = -1;
        return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock());
      }
    }
  }

  /**
   * This util is used to canonicalize row generated from V1 engine, which is stored using
   * {@link DataSchema#getStoredColumnDataTypes()} format. However, the transferable block ser/de stores data in the
   * {@link DataSchema#getColumnDataTypes()} format.
   *
   * @param row un-canonicalize row.
   * @param dataSchema data schema desired for the row.
   * @return canonicalize row.
   */
  private static Object[] canonicalizeRow(Object[] row, DataSchema dataSchema) {
    Object[] resultRow = new Object[row.length];
    for (int colId = 0; colId < row.length; colId++) {
      resultRow[colId] = dataSchema.getColumnDataType(colId).convert(row[colId]);
    }
    return resultRow;
  }

  /**
   * we re-arrange columns to match the projection in the case of order by - this is to ensure
   * that V1 results match what the expected projection schema in the calcite logical operator; if
   * we realize that there are other situations where we need to post-process v1 results to adhere to
   * the expected results we should factor this out and also apply the canonicalization of the data
   * types during this post-process step (also see LeafStageTransferableBlockOperator#canonicalizeRow)
   *
   * @param serverResultsBlock result block from leaf stage
   * @param dataSchema the desired schema for send operator
   * @return conformed collection of rows.
   */
  @SuppressWarnings("ConstantConditions")
  private static List<Object[]> cleanUpDataBlock(InstanceResponseBlock serverResultsBlock, DataSchema dataSchema,
      boolean requiresCleanUp) {
    // Extract the result rows
    Collection<Object[]> resultRows = serverResultsBlock.getRows();
    List<Object[]> extractedRows = new ArrayList<>(resultRows.size());
    if (requiresCleanUp) {
      DataSchema resultSchema = serverResultsBlock.getDataSchema();
      List<String> selectionColumns =
          SelectionOperatorUtils.getSelectionColumns(serverResultsBlock.getQueryContext(), resultSchema);
      int[] columnIndices = SelectionOperatorUtils.getColumnIndices(selectionColumns, resultSchema);
      DataSchema adjustedDataSchema = SelectionOperatorUtils.getSchemaForProjection(resultSchema, columnIndices);
      Preconditions.checkState(isDataSchemaColumnTypesCompatible(dataSchema.getColumnDataTypes(),
              adjustedDataSchema.getColumnDataTypes()),
          "Incompatible result data schema: " + "Expecting: " + dataSchema + " Actual: " + adjustedDataSchema);
      int numColumns = columnIndices.length;

      if (serverResultsBlock.getQueryContext().getOrderByExpressions() != null) {
        // extract result row in ordered fashion
        PriorityQueue<Object[]> priorityQueue = (PriorityQueue<Object[]>) resultRows;
        while (!priorityQueue.isEmpty()) {
          Object[] row = priorityQueue.poll();
          assert row != null;
          Object[] extractedRow = new Object[numColumns];
          for (int colId = 0; colId < numColumns; colId++) {
            Object value = row[columnIndices[colId]];
            if (value != null) {
              extractedRow[colId] = dataSchema.getColumnDataType(colId).convert(value);
            }
          }
          extractedRows.add(extractedRow);
        }
      } else {
        // extract result row in non-ordered fashion
        for (Object[] row : resultRows) {
          assert row != null;
          Object[] extractedRow = new Object[numColumns];
          for (int colId = 0; colId < numColumns; colId++) {
            Object value = row[columnIndices[colId]];
            if (value != null) {
              extractedRow[colId] = dataSchema.getColumnDataType(colId).convert(value);
            }
          }
          extractedRows.add(extractedRow);
        }
      }
    } else {
      if (resultRows instanceof List) {
        for (Object[] orgRow : resultRows) {
          extractedRows.add(canonicalizeRow(orgRow, dataSchema));
        }
      } else if (resultRows instanceof PriorityQueue) {
        PriorityQueue<Object[]> priorityQueue = (PriorityQueue<Object[]>) resultRows;
        while (!priorityQueue.isEmpty()) {
          extractedRows.add(canonicalizeRow(priorityQueue.poll(), dataSchema));
        }
      } else {
        throw new UnsupportedOperationException("Unsupported collection type: " + resultRows.getClass());
      }
    }
    return extractedRows;
  }

  /**
   * @see LeafStageTransferableBlockOperator#cleanUpDataBlock(InstanceResponseBlock, DataSchema, boolean)
   */
  @SuppressWarnings("ConstantConditions")
  private static DataSchema cleanUpDataSchema(InstanceResponseBlock serverResultsBlock, DataSchema desiredDataSchema) {
    DataSchema resultSchema = serverResultsBlock.getDataSchema();
    List<String> selectionColumns =
        SelectionOperatorUtils.getSelectionColumns(serverResultsBlock.getQueryContext(), resultSchema);

    int[] columnIndices = SelectionOperatorUtils.getColumnIndices(selectionColumns, resultSchema);
    DataSchema adjustedResultSchema = SelectionOperatorUtils.getSchemaForProjection(resultSchema, columnIndices);
    Preconditions.checkState(isDataSchemaColumnTypesCompatible(desiredDataSchema.getColumnDataTypes(),
            adjustedResultSchema.getColumnDataTypes()),
        "Incompatible result data schema: " + "Expecting: " + desiredDataSchema + " Actual: " + adjustedResultSchema);
    return adjustedResultSchema;
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
