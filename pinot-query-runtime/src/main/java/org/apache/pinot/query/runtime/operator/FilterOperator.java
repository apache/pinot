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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.BooleanUtils;
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
public class FilterOperator extends MultiStageOperator<MultiStageOperator.BaseStatKeys> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterOperator.class);
  private static final String EXPLAIN_NAME = "FILTER";

  private final MultiStageOperator<?> _upstreamOperator;
  private final TransformOperand _filterOperand;
  private final DataSchema _dataSchema;

  public FilterOperator(OpChainExecutionContext context, MultiStageOperator<?> upstreamOperator, DataSchema dataSchema,
      RexExpression filter) {
    super(context);
    _upstreamOperator = upstreamOperator;
    _dataSchema = dataSchema;
    _filterOperand = TransformOperandFactory.getTransformOperand(filter, dataSchema);
    Preconditions.checkState(_filterOperand.getResultType() == ColumnDataType.BOOLEAN,
        "Filter operand must return BOOLEAN, got: %s", _filterOperand.getResultType());
  }

  @Override
  public Class<BaseStatKeys> getStatKeyClass() {
    return BaseStatKeys.class;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator<?>> getChildOperators() {
    return ImmutableList.of(_upstreamOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    TransferableBlock block = _upstreamOperator.nextBlock();
    if (block.isEndOfStreamBlock()) {
      return block;
    }
    List<Object[]> resultRows = new ArrayList<>();
    for (Object[] row : block.getContainer()) {
      Object filterResult = _filterOperand.apply(row);
      if (BooleanUtils.isTrueInternalValue(filterResult)) {
        resultRows.add(row);
      }
    }
    return new TransferableBlock(resultRows, _dataSchema, DataBlock.Type.ROW);
  }
}
