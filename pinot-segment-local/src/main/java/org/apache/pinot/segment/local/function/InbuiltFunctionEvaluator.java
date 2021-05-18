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
package org.apache.pinot.segment.local.function;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Evaluates an expression.
 * <p>This is optimized for evaluating an expression multiple times with different inputs.
 * <p>Overall idea: parse the expression into an ExecutableNode, where an ExecutableNode can be:
 * <ul>
 *   <li>FunctionNode - executes a function</li>
 *   <li>ColumnNode - fetches the value of the column from the input GenericRow</li>
 *   <li>ConstantNode - returns the literal value</li>
 * </ul>
 */
public class InbuiltFunctionEvaluator implements FunctionEvaluator {
  // Root of the execution tree
  private final ExecutableNode _rootNode;
  private final List<String> _arguments;

  public InbuiltFunctionEvaluator(String functionExpression) {
    _arguments = new ArrayList<>();
    _rootNode = planExecution(RequestContextUtils.getExpressionFromSQL(functionExpression));
  }

  private ExecutableNode planExecution(ExpressionContext expression) {
    switch (expression.getType()) {
      case LITERAL:
        return new ConstantExecutionNode(expression.getLiteral());
      case IDENTIFIER:
        String columnName = expression.getIdentifier();
        ColumnExecutionNode columnExecutionNode = new ColumnExecutionNode(columnName, _arguments.size());
        _arguments.add(columnName);
        return columnExecutionNode;
      case FUNCTION:
        FunctionContext function = expression.getFunction();
        List<ExpressionContext> arguments = function.getArguments();
        int numArguments = arguments.size();
        ExecutableNode[] childNodes = new ExecutableNode[numArguments];
        for (int i = 0; i < numArguments; i++) {
          childNodes[i] = planExecution(arguments.get(i));
        }
        String functionName = function.getFunctionName();
        FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName, numArguments);
        Preconditions.checkState(functionInfo != null, "Unsupported function: %s with %s parameters", functionName,
            numArguments);
        return new FunctionExecutionNode(functionInfo, childNodes);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public List<String> getArguments() {
    return _arguments;
  }

  @Override
  public Object evaluate(GenericRow row) {
    return _rootNode.execute(row);
  }

  @Override
  public Object evaluate(Object[] values) {
    return _rootNode.execute(values);
  }

  private interface ExecutableNode {

    Object execute(GenericRow row);

    Object execute(Object[] values);
  }

  private static class FunctionExecutionNode implements ExecutableNode {
    final FunctionInvoker _functionInvoker;
    final ExecutableNode[] _argumentNodes;
    final Object[] _arguments;

    FunctionExecutionNode(FunctionInfo functionInfo, ExecutableNode[] argumentNodes) {
      _functionInvoker = new FunctionInvoker(functionInfo);
      _argumentNodes = argumentNodes;
      _arguments = new Object[_argumentNodes.length];
    }

    @Override
    public Object execute(GenericRow row) {
      int numArguments = _argumentNodes.length;
      for (int i = 0; i < numArguments; i++) {
        _arguments[i] = _argumentNodes[i].execute(row);
      }
      _functionInvoker.convertTypes(_arguments);
      return _functionInvoker.invoke(_arguments);
    }

    @Override
    public Object execute(Object[] values) {
      int numArguments = _argumentNodes.length;
      for (int i = 0; i < numArguments; i++) {
        _arguments[i] = _argumentNodes[i].execute(values);
      }
      _functionInvoker.convertTypes(_arguments);
      return _functionInvoker.invoke(_arguments);
    }
  }

  private static class ConstantExecutionNode implements ExecutableNode {
    final String _value;

    ConstantExecutionNode(String value) {
      _value = value;
    }

    @Override
    public String execute(GenericRow row) {
      return _value;
    }

    @Override
    public Object execute(Object[] values) {
      return _value;
    }
  }

  private static class ColumnExecutionNode implements ExecutableNode {
    final String _column;
    final int _id;

    ColumnExecutionNode(String column, int id) {
      _column = column;
      _id = id;
    }

    @Override
    public Object execute(GenericRow row) {
      return row.getValue(_column);
    }

    @Override
    public Object execute(Object[] values) {
      return values[_id];
    }
  }
}
