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
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;


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
public class TransformOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "TRANSFORM";
  private final Operator<TransferableBlock> _upstreamOperator;
  private final List<TransformOperand> _transformOperandsList;
  private final int _resultColumnSize;
  // TODO: Check type matching between resultSchema and the actual result.
  private final DataSchema _resultSchema;
  private TransferableBlock _upstreamErrorBlock;

  public TransformOperator(Operator<TransferableBlock> upstreamOperator, DataSchema resultSchema,
      List<RexExpression> transforms, DataSchema upstreamDataSchema) {
    Preconditions.checkState(!transforms.isEmpty(), "transform operand should not be empty.");
    Preconditions.checkState(resultSchema.size() == transforms.size(),
        "result schema size:" + resultSchema.size() + " doesn't match transform operand size:" + transforms.size());
    _upstreamOperator = upstreamOperator;
    _resultColumnSize = transforms.size();
    _transformOperandsList = new ArrayList<>(_resultColumnSize);
    for (RexExpression rexExpression : transforms) {
      _transformOperandsList.add(TransformOperand.toTransformOperand(rexExpression, upstreamDataSchema));
    }
    _resultSchema = resultSchema;
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
    if (block.isErrorBlock()) {
      _upstreamErrorBlock = block;
    }
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }

    if (TransferableBlockUtils.isEndOfStream(block) || TransferableBlockUtils.isNoOpBlock(block)) {
      return block;
    }

    List<Object[]> resultRows = new ArrayList<>();
    List<Object[]> container = block.getContainer();
    for (Object[] row : container) {
      Object[] resultRow = new Object[_resultColumnSize];
      for (int i = 0; i < _resultColumnSize; i++) {
        resultRow[i] = _transformOperandsList.get(i).apply(row);
      }
      resultRows.add(resultRow);
    }
    return new TransferableBlock(resultRows, _resultSchema, DataBlock.Type.ROW);
  }
}
