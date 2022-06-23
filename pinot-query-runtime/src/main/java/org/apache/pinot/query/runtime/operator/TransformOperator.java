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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.datablock.BaseDataBlock;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


/**
 * This basic {@code TransformOperator} implement basic transformations.
 */
// TODO: add FunctionCall support.
public class TransformOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "TRANSFORM";
  private final BaseOperator<TransferableBlock> _upstreamOperator;
  private final List<RexExpression> _transforms;
  private final int _resultColumnSize;

  public TransformOperator(BaseOperator<TransferableBlock> upstreamOperator, List<RexExpression> transforms) {
    _upstreamOperator = upstreamOperator;
    _transforms = transforms;
    _resultColumnSize = _transforms.size();
  }

  @Override
  public List<Operator> getChildOperators() {
    // WorkerExecutor doesn't use getChildOperators, returns null here.
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    try {
      return transform(_upstreamOperator.nextBlock());
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private TransferableBlock transform(TransferableBlock block)
      throws Exception {
    if (TransferableBlockUtils.isEndOfStream(block)) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
    List<Object[]> resultRows = new ArrayList<>();
    DataSchema dataSchema = block.getDataSchema();
    List<Object[]> container = block.getContainer();
    for (Object[] row : container) {
      Object[] resultRow = new Object[_resultColumnSize];
      for (int i = 0; i < _resultColumnSize; i++) {
        resultRow[i] = applyTransform(_transforms.get(i), dataSchema, row);
      }
      resultRows.add(resultRow);
    }
    return new TransferableBlock(resultRows, computeSchema(dataSchema), BaseDataBlock.Type.ROW);
  }

  private Object applyTransform(RexExpression rexExpression, DataSchema dataSchema, Object[] row) {
    if (rexExpression instanceof RexExpression.InputRef) {
      RexExpression.InputRef inputRef = (RexExpression.InputRef) rexExpression;
      return row[inputRef.getIndex()];
    } else {
      throw new UnsupportedOperationException("Unsupported Rex Expression: " + rexExpression);
    }
  }

  private DataSchema computeSchema(DataSchema dataSchema) {
    String[] columnNames = new String[_resultColumnSize];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[_resultColumnSize];
    for (int i = 0; i < _resultColumnSize; i++) {
      columnNames[i] = computeColumnName(_transforms.get(i), dataSchema);
      columnDataTypes[i] = computeColumnType(_transforms.get(i), dataSchema);
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  private DataSchema.ColumnDataType computeColumnType(RexExpression rexExpression, DataSchema dataSchema) {
    if (rexExpression instanceof RexExpression.InputRef) {
      RexExpression.InputRef inputRef = (RexExpression.InputRef) rexExpression;
      return dataSchema.getColumnDataType(inputRef.getIndex());
    } else {
      throw new UnsupportedOperationException("Unsupported Rex Expression: " + rexExpression);
    }
  }

  private String computeColumnName(RexExpression rexExpression, DataSchema dataSchema) {
    if (rexExpression instanceof RexExpression.InputRef) {
      RexExpression.InputRef inputRef = (RexExpression.InputRef) rexExpression;
      return dataSchema.getColumnName(inputRef.getIndex());
    } else {
      throw new UnsupportedOperationException("Unsupported Rex Expression: " + rexExpression);
    }
  }
}
