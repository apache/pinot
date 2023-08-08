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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
   FilterOperator apply filter on rows from upstreamOperator.
   There are three types of filter operands
   1) inputRef
   2) Literal
   3) FunctionOperand
   All three types' result has to be a boolean to be used to filter rows.
   FunctionOperand supports,
    1) AND, OR, NOT functions to combine operands.
    2) Binary Operand: equals, notEquals, greaterThan, greaterThanOrEqual, lessThan, lessThanOrEqual
    3) All boolean scalar functions we have that take tranformOperand.
    Note: Scalar functions are the ones we have in v1 engine and only do function name and arg # matching.
 */
public class FilterOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "FILTER";
  private final MultiStageOperator _upstreamOperator;
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateOperator.class);
  private final TransformOperand _filterOperand;
  private final DataSchema _dataSchema;
  private TransferableBlock _upstreamErrorBlock;

  public FilterOperator(OpChainExecutionContext context, MultiStageOperator upstreamOperator, DataSchema dataSchema,
      RexExpression filter) {
    super(context);
    _upstreamOperator = upstreamOperator;
    _dataSchema = dataSchema;
    _filterOperand = TransformOperand.toTransformOperand(filter, dataSchema);
    _upstreamErrorBlock = null;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_upstreamOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    try {
      TransferableBlock block = _upstreamOperator.nextBlock();
      return transform(block);
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  @SuppressWarnings("ConstantConditions")
  private TransferableBlock transform(TransferableBlock block)
      throws Exception {
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    } else if (block.isErrorBlock()) {
      _upstreamErrorBlock = block;
      return _upstreamErrorBlock;
    } else if (TransferableBlockUtils.isEndOfStream(block) || TransferableBlockUtils.isNoOpBlock(block)) {
      return block;
    }

    List<Object[]> resultRows = new ArrayList<>();
    List<Object[]> container = block.getContainer();
    for (Object[] row : container) {
      if ((Boolean) TypeUtils.convert(_filterOperand.apply(row), DataSchema.ColumnDataType.BOOLEAN)) {
        resultRows.add(row);
      }
    }
    return new TransferableBlock(resultRows, _dataSchema, DataBlock.Type.ROW);
  }
}
