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
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LiteralValueOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "LITERAL_VALUE_PROVIDER";
  private static final Logger LOGGER = LoggerFactory.getLogger(LiteralValueOperator.class);

  private final DataSchema _dataSchema;
  private final List<List<RexExpression.Literal>> _literalRows;
  private boolean _isLiteralBlockReturned;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public LiteralValueOperator(OpChainExecutionContext context, ValueNode node) {
    super(context);
    _dataSchema = node.getDataSchema();
    _literalRows = node.getLiteralRows();
    // Only return a single literal block when it is the 1st virtual server. Otherwise, result will be duplicated.
    _isLiteralBlockReturned = context.getId().getVirtualServerId() != 0;
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
    return List.of();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (!_isLiteralBlockReturned && !_literalRows.isEmpty()) {
      _isLiteralBlockReturned = true;
      return constructBlock();
    } else {
      return createEosBlock();
    }
  }

  protected TransferableBlock createEosBlock() {
    return TransferableBlockUtils.getEndOfStreamTransferableBlock(
        MultiStageQueryStats.createLiteral(_context.getStageId(), _statMap));
  }

  @Override
  public Type getOperatorType() {
    return Type.LITERAL;
  }

  private TransferableBlock constructBlock() {
    List<Object[]> blockContent = new ArrayList<>(_literalRows.size());
    for (List<RexExpression.Literal> row : _literalRows) {
      Object[] values = new Object[_dataSchema.size()];
      for (int i = 0; i < _dataSchema.size(); i++) {
        values[i] = row.get(i).getValue();
      }
      blockContent.add(values);
    }
    return new TransferableBlock(blockContent, _dataSchema, DataBlock.Type.ROW);
  }

  public enum StatKey implements StatMap.Key {
    //@formatter:off
    EXECUTION_TIME_MS(StatMap.Type.LONG),
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
