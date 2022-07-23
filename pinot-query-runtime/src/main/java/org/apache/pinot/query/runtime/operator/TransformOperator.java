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
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.datablock.BaseDataBlock;
import org.apache.pinot.core.common.datablock.DataBlockUtils;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


/**
 * This basic {@code TransformOperator} implement basic transformations.
 */
public class TransformOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "TRANSFORM";
  private final BaseOperator<TransferableBlock> _upstreamOperator;
  private final List<TransformOperands> _transformOperandsList;
  private final int _resultColumnSize;
  private final DataSchema _resultSchema;
  private TransferableBlock _upstreamErrorBlock;

  public TransformOperator(BaseOperator<TransferableBlock> upstreamOperator, DataSchema dataSchema,
      List<RexExpression> transforms, DataSchema upstreamDataSchema) {
    _upstreamOperator = upstreamOperator;
    _resultColumnSize = transforms.size();
    _transformOperandsList = new ArrayList<>(_resultColumnSize);
    for (RexExpression rexExpression : transforms) {
      _transformOperandsList.add(TransformOperands.toFunctionOperands(rexExpression, upstreamDataSchema));
    }
    _resultSchema = dataSchema;
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
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }
    if (!TransferableBlockUtils.isEndOfStream(block)) {
      List<Object[]> resultRows = new ArrayList<>();
      List<Object[]> container = block.getContainer();
      for (Object[] row : container) {
        Object[] resultRow = new Object[_resultColumnSize];
        for (int i = 0; i < _resultColumnSize; i++) {
          resultRow[i] = _transformOperandsList.get(i).apply(row);
        }
        resultRows.add(resultRow);
      }
      return new TransferableBlock(resultRows, _resultSchema, BaseDataBlock.Type.ROW);
    } else if (block.isErrorBlock()) {
      _upstreamErrorBlock = block;
      return _upstreamErrorBlock;
    } else {
      return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(_resultSchema));
    }
  }

  private static abstract class TransformOperands {
    protected String _resultName;
    protected DataSchema.ColumnDataType _resultType;

    public static TransformOperands toFunctionOperands(RexExpression rexExpression, DataSchema dataSchema) {
      if (rexExpression instanceof RexExpression.InputRef) {
        return new ReferenceOperands((RexExpression.InputRef) rexExpression, dataSchema);
      } else if (rexExpression instanceof RexExpression.FunctionCall) {
        return new FunctionOperands((RexExpression.FunctionCall) rexExpression, dataSchema);
      } else {
        throw new UnsupportedOperationException("Unsupported RexExpression: " + rexExpression);
      }
    }

    public String getResultName() {
      return _resultName;
    }

    public DataSchema.ColumnDataType getResultType() {
      return _resultType;
    }

    public abstract Object apply(Object[] row);
  }

  private static class FunctionOperands extends TransformOperands {
    private final List<TransformOperands> _childOperandList;
    private final FunctionInvoker _functionInvoker;
    private final Object[] _reusableOperandHolder;

    public FunctionOperands(RexExpression.FunctionCall functionCall, DataSchema dataSchema) {
      // iteratively resolve child operands.
      List<RexExpression> operandExpressions = functionCall.getFunctionOperands();
      _childOperandList = new ArrayList<>(operandExpressions.size());
      for (RexExpression childRexExpression : operandExpressions) {
        _childOperandList.add(toFunctionOperands(childRexExpression, dataSchema));
      }
      FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(
          OperatorUtils.canonicalizeFunctionName(functionCall.getFunctionName()), operandExpressions.size());
      Preconditions.checkNotNull(functionInfo, "Cannot find function with Name: "
          + functionCall.getFunctionName());
      _functionInvoker = new FunctionInvoker(functionInfo);
      _resultName = computeColumnName(functionCall.getFunctionName(), _childOperandList);
      _resultType = FunctionUtils.getColumnDataType(_functionInvoker.getResultClass());
      _reusableOperandHolder = new Object[operandExpressions.size()];
    }

    @Override
    public Object apply(Object[] row) {
      for (int i = 0; i < _childOperandList.size(); i++) {
        _reusableOperandHolder[i] = _childOperandList.get(i).apply(row);
      }
      return _functionInvoker.invoke(_reusableOperandHolder);
    }

    private static String computeColumnName(String functionName, List<TransformOperands> childOperands) {
      StringBuilder sb = new StringBuilder();
      sb.append(functionName);
      sb.append("(");
      for (TransformOperands operands : childOperands) {
        sb.append(operands.getResultName());
        sb.append(",");
      }
      sb.append(")");
      return sb.toString();
    }
  }

  private static class ReferenceOperands extends TransformOperands {
    private final int _refIndex;

    public ReferenceOperands(RexExpression.InputRef inputRef, DataSchema dataSchema) {
      _refIndex = inputRef.getIndex();
      _resultType = dataSchema.getColumnDataType(_refIndex);
      _resultName = dataSchema.getColumnName(_refIndex);
    }

    @Override
    public Object apply(Object[] row) {
      return row[_refIndex];
    }
  }
}
