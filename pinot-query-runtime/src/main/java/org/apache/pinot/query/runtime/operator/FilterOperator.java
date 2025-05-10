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
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
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
public class FilterOperator extends MultiStageOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterOperator.class);
  private static final String EXPLAIN_NAME = "FILTER";

  private final MultiStageOperator _input;
  private final TransformOperand _filterOperand;
  private final DataSchema _dataSchema;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public FilterOperator(OpChainExecutionContext context, MultiStageOperator input, FilterNode node) {
    super(context);
    _input = input;
    _dataSchema = node.getDataSchema();
    _filterOperand = TransformOperandFactory.getTransformOperand(node.getCondition(), _dataSchema);
    Preconditions.checkState(_filterOperand.getResultType() == ColumnDataType.BOOLEAN,
        "Filter operand must return BOOLEAN, got: %s", _filterOperand.getResultType());
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public Type getOperatorType() {
    return Type.FILTER;
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
    // Keep reading the input blocks until we find a match row or all blocks are processed.
    // TODO: Consider batching the rows to improve performance.
    while (true) {
      MseBlock block = _input.nextBlock();
      if (block.isEos()) {
        return block;
      }
      MseBlock.Data dataBlock = (MseBlock.Data) block;
      List<Object[]> rows = new ArrayList<>();
      for (Object[] row : dataBlock.asRowHeap().getRows()) {
        Object filterResult = _filterOperand.apply(row);
        if (BooleanUtils.isTrueInternalValue(filterResult)) {
          rows.add(row);
        }
      }
      if (!rows.isEmpty()) {
        return new RowHeapDataBlock(rows, _dataSchema);
      }
    }
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  public enum StatKey implements StatMap.Key {
    //@formatter:off
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
    };
    //@formatter:on

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
