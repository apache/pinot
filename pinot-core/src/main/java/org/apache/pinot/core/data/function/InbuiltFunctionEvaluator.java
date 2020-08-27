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
package org.apache.pinot.core.data.function;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Evaluates a function expression.
 * <p>This is optimized for evaluating the an expression multiple times with different inputs.
 * <p>Overall idea
 * <ul>
 *   <li>Parse the function expression into an expression tree</li>
 *   <li>Convert each node in the expression tree into and ExecutableNode</li>
 * </ul>
 * <p>An ExecutableNode can be a
 * <ul>
 *   <li>FunctionNode - executes another function</li>
 *   <li>ColumnNode - fetches the value of the column from the input GenericRow</li>
 *   <li>
 *     ConstantNode - returns the same value
 *     <p>Typically constant function arguments are represented using a ConstantNode
 *   </li>
 * </ul>
 */
public class InbuiltFunctionEvaluator implements FunctionEvaluator {
  // Root of the execution tree
  private final ExecutableNode _rootNode;
  private final List<String> _arguments;

  public InbuiltFunctionEvaluator(String functionExpression) {
    _arguments = new ArrayList<>();
    ExpressionContext expression = QueryContextConverterUtils.getExpression(functionExpression);
    Preconditions
        .checkArgument(expression.getType() == ExpressionContext.Type.FUNCTION, "Invalid function expression: %s",
            functionExpression);
    _rootNode = planExecution(expression.getFunction());
  }

  private FunctionExecutionNode planExecution(FunctionContext function) {
    List<ExpressionContext> arguments = function.getArguments();
    int numArguments = arguments.size();
    ExecutableNode[] childNodes = new ExecutableNode[numArguments];
    for (int i = 0; i < numArguments; i++) {
      ExpressionContext argument = arguments.get(i);
      ExecutableNode childNode;
      switch (argument.getType()) {
        case FUNCTION:
          childNode = planExecution(argument.getFunction());
          break;
        case IDENTIFIER:
          String columnName = argument.getIdentifier();
          childNode = new ColumnExecutionNode(columnName);
          _arguments.add(columnName);
          break;
        case LITERAL:
          childNode = new ConstantExecutionNode(argument.getLiteral());
          break;
        default:
          throw new IllegalStateException();
      }
      childNodes[i] = childNode;
    }

    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(function.getFunctionName(), numArguments);
    Preconditions
        .checkState(functionInfo != null, "Unsupported function: %s with %s parameters", function.getFunctionName(),
            numArguments);
    return new FunctionExecutionNode(functionInfo, childNodes);
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
  public Object evaluate(Object[] arguments) {
    return _rootNode.execute(arguments);
  }

  private interface ExecutableNode {

    /**
     * Execute the function by extracting arguments from the row
     */
    Object execute(GenericRow row);

    /**
     * Execute the function on provided arguments
     */
    Object execute(Object[] arguments);
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
    public Object execute(Object[] arguments) {
      _functionInvoker.convertTypes(arguments);
      return _functionInvoker.invoke(arguments);
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
    public String execute(Object[] arguments) { return _value; }
  }

  private static class ColumnExecutionNode implements ExecutableNode {
    final String _column;

    ColumnExecutionNode(String column) {
      _column = column;
    }

    @Override
    public Object execute(GenericRow row) {
      return row.getValue(_column);
    }

    @Override
    public Object execute(Object[] arguments) {
      throw new UnsupportedOperationException("Operation not supported for ColumnExecutionNode");
    }
  }
}
