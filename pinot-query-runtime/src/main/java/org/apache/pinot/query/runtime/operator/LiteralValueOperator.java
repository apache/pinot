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
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LiteralValueOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "LITERAL_VALUE_PROVIDER";
  private static final Logger LOGGER = LoggerFactory.getLogger(LiteralValueOperator.class);

  private final DataSchema _dataSchema;
  private final TransferableBlock _rexLiteralBlock;
  private boolean _isLiteralBlockReturned;

  public LiteralValueOperator(OpChainExecutionContext context, DataSchema dataSchema,
      List<List<RexExpression>> rexLiteralRows) {
    super(context);
    _dataSchema = dataSchema;
    _rexLiteralBlock = constructBlock(rexLiteralRows);
    // only return a single literal block when it is the 1st virtual server. otherwise, result will be duplicated.
    _isLiteralBlockReturned = context.getId().getVirtualServerId() != 0;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
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
    if (!_isLiteralBlockReturned && !_isEarlyTerminated) {
      _isLiteralBlockReturned = true;
      return _rexLiteralBlock;
    } else {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
  }

  private TransferableBlock constructBlock(List<List<RexExpression>> rexLiteralRows) {
    List<Object[]> blockContent = new ArrayList<>();
    for (List<RexExpression> rexLiteralRow : rexLiteralRows) {
      Object[] row = new Object[_dataSchema.size()];
      for (int i = 0; i < _dataSchema.size(); i++) {
        row[i] = ((RexExpression.Literal) rexLiteralRow.get(i)).getValue();
      }
      blockContent.add(row);
    }
    return new TransferableBlock(blockContent, _dataSchema, DataBlock.Type.ROW);
  }
}
