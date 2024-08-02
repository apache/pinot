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
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This basic {@code TransformOperator} implement basic transformations.
 *
 * This operator performs three kinds of transform
 * - InputRef transform, which reads from certain input column based on column index
 * - Literal transform, which outputs literal value
 * - Function transform, which runs a function on function operands. Function operands and be any of 3 the transform.
 * Note: Function transform only runs functions from v1 engine scalar function factory, which only does argument count
 * and canonicalized function name matching (lower case).
 */
public class TransformOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformOperator.class);
  private static final String EXPLAIN_NAME = "TRANSFORM";

  private final MultiStageOperator _input;
  private final List<TransformOperand> _transformOperandsList;
  private final int _resultColumnSize;
  // TODO: Check type matching between resultSchema and the actual result.
  private final DataSchema _resultSchema;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public TransformOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema inputSchema,
      ProjectNode node) {
    super(context);
    _input = input;
    List<RexExpression> projects = node.getProjects();
    _resultColumnSize = projects.size();
    _transformOperandsList = new ArrayList<>(_resultColumnSize);
    for (RexExpression rexExpression : projects) {
      _transformOperandsList.add(TransformOperandFactory.getTransformOperand(rexExpression, inputSchema));
    }
    _resultSchema = node.getDataSchema();
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
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
    return Type.TRANSFORM;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    TransferableBlock block = _input.nextBlock();
    if (block.isEndOfStreamBlock()) {
      if (block.isSuccessfulEndOfStreamBlock()) {
        return updateEosBlock(block, _statMap);
      } else {
        return block;
      }
    }
    List<Object[]> container = block.getContainer();
    List<Object[]> resultRows = new ArrayList<>(container.size());
    for (Object[] row : container) {
      Object[] resultRow = new Object[_resultColumnSize];
      for (int i = 0; i < _resultColumnSize; i++) {
        resultRow[i] = _transformOperandsList.get(i).apply(row);
      }
      resultRows.add(resultRow);
    }
    return new TransferableBlock(resultRows, _resultSchema, DataBlock.Type.ROW);
  }

  public enum StatKey implements StatMap.Key {
    //@formatter:off
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG);
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
