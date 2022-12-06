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
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
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
  private int _currentIndex;

  public LeafStageTransferableBlockOperator(List<InstanceResponseBlock> baseResultBlock, DataSchema dataSchema) {
    _baseResultBlock = baseResultBlock;
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
        BaseResultsBlock resultsBlock = responseBlock.getResultsBlock();
        if (resultsBlock != null) {
          List<Object[]> rows =
              toList(resultsBlock.getRows(responseBlock.getQueryContext()), responseBlock.getDataSchema());
          return new TransferableBlock(rows, responseBlock.getDataSchema(), DataBlock.Type.ROW);
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

  private static List<Object[]> toList(Collection<Object[]> collection, DataSchema dataSchema) {
    if (collection == null || collection.isEmpty()) {
      return new ArrayList<>();
    }
    List<Object[]> resultRows = new ArrayList<>(collection.size());
    if (collection instanceof List) {
      for (Object[] orgRow : collection) {
        resultRows.add(canonicalizeRow(orgRow, dataSchema));
      }
    } else if (collection instanceof PriorityQueue) {
      PriorityQueue<Object[]> priorityQueue = (PriorityQueue<Object[]>) collection;
      while (!priorityQueue.isEmpty()) {
        resultRows.add(canonicalizeRow(priorityQueue.poll(), dataSchema));
      }
    } else {
      throw new UnsupportedOperationException("Unsupported collection type: " + collection.getClass());
    }
    return resultRows;
  }
}
