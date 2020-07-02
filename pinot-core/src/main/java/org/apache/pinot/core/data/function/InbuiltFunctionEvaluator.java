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
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
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

  public InbuiltFunctionEvaluator(String expression)
      throws Exception {
    _arguments = new ArrayList<>();
    _rootNode = planExecution(TransformExpressionTree.compileToExpressionTree(expression));
  }

  private ExecutableNode planExecution(TransformExpressionTree expressionTree)
      throws Exception {
    String transformName = expressionTree.getValue();
    List<TransformExpressionTree> children = expressionTree.getChildren();
    Class<?>[] argumentTypes = new Class<?>[children.size()];
    ExecutableNode[] childNodes = new ExecutableNode[children.size()];
    for (int i = 0; i < children.size(); i++) {
      TransformExpressionTree childExpression = children.get(i);
      ExecutableNode childNode;
      switch (childExpression.getExpressionType()) {
        case FUNCTION:
          childNode = planExecution(childExpression);
          break;
        case IDENTIFIER:
          String columnName = childExpression.getValue();
          childNode = new ColumnExecutionNode(columnName);
          _arguments.add(columnName);
          break;
        case LITERAL:
          childNode = new ConstantExecutionNode(childExpression.getValue());
          break;
        default:
          throw new UnsupportedOperationException("Unsupported expression type:" + childExpression.getExpressionType());
      }
      childNodes[i] = childNode;
      argumentTypes[i] = childNode.getReturnType();
    }

    FunctionInfo functionInfo =
        FunctionRegistry.getFunctionByNameWithApplicableArgumentTypes(transformName, argumentTypes);
    return new FunctionExecutionNode(functionInfo, childNodes);
  }

  @Override
  public List<String> getArguments() {
    return _arguments;
  }

  public Object evaluate(GenericRow row) {
    return _rootNode.execute(row);
  }

  private interface ExecutableNode {

    Object execute(GenericRow row);

    Class<?> getReturnType();
  }

  private static class FunctionExecutionNode implements ExecutableNode {
    FunctionInvoker _functionInvoker;
    ExecutableNode[] _argumentProviders;
    Object[] _argInputs;

    public FunctionExecutionNode(FunctionInfo functionInfo, ExecutableNode[] argumentProviders)
        throws Exception {
      Preconditions.checkNotNull(functionInfo);
      Preconditions.checkNotNull(argumentProviders);
      _functionInvoker = new FunctionInvoker(functionInfo);
      _argumentProviders = argumentProviders;
      _argInputs = new Object[_argumentProviders.length];
    }

    public Object execute(GenericRow row) {
      for (int i = 0; i < _argumentProviders.length; i++) {
        _argInputs[i] = _argumentProviders[i].execute(row);
      }
      return _functionInvoker.process(_argInputs);
    }

    public Class<?> getReturnType() {
      return _functionInvoker.getReturnType();
    }
  }

  private static class ConstantExecutionNode implements ExecutableNode {
    private Object _value;
    private Class<?> _returnType;

    public ConstantExecutionNode(String value) {
      if (NumberUtils.isCreatable(value)) {
        _value = NumberUtils.createNumber(value);
        _returnType = Number.class;
      } else {
        _value = value;
        _returnType = String.class;
      }
    }

    @Override
    public Object execute(GenericRow row) {
      return _value;
    }

    @Override
    public Class<?> getReturnType() {
      return _returnType;
    }
  }

  private static class ColumnExecutionNode implements ExecutableNode {
    private String _column;

    public ColumnExecutionNode(String column) {
      _column = column;
    }

    @Override
    public Object execute(GenericRow row) {
      return row.getValue(_column);
    }

    @Override
    public Class<?> getReturnType() {
      return Object.class;
    }
  }
}
