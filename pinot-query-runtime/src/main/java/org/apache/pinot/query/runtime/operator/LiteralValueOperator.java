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
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


public class LiteralValueOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "LITERAL_VALUE_PROVIDER";

  private final DataSchema _dataSchema;
  private final TransferableBlock _rexLiteralBlock;
  private boolean _isLiteralBlockReturned;

  public LiteralValueOperator(DataSchema dataSchema, List<List<RexExpression>> rexLiteralRows) {
    _dataSchema = dataSchema;
    _rexLiteralBlock = constructBlock(rexLiteralRows);
    _isLiteralBlockReturned = false;
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
    if (!_isLiteralBlockReturned) {
      _isLiteralBlockReturned = true;
      return _rexLiteralBlock;
    } else {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock(_dataSchema);
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
    return new TransferableBlock(blockContent, _dataSchema, BaseDataBlock.Type.ROW);
  }
}
