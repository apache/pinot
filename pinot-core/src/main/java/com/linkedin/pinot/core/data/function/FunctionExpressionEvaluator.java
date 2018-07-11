/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.function;

import java.util.List;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;

/**
 * Evaluates a function expression. This is optimized 
 * for evaluating the an expression multiple times with different inputs.
 * Overall idea 
 *  - Parse the function expression into an expression tree
 *  - Convert each node in the expression tree into and ExecutableNode
 *  - An ExecutableNode can be a 
 *      - FunctionNode - That executes another function
 *      - ColumnNode - fetches the value of the column from the input GenericRow
 *      - ConstantNode - returns the same value - typically constant function arguments are represented using a ConstantNode
 */
public class FunctionExpressionEvaluator {
  /**
   * PQL compilers are not thread safe. Creating one per thread
   */
  static ThreadLocal<Pql2Compiler> COMPILER_CACHE = new ThreadLocal<Pql2Compiler>() {
    protected Pql2Compiler initialValue() {
      return new Pql2Compiler();
    };
  };
  /**
   * Root of the execution tree
   */
  ExecutableNode _rootNode;

  public FunctionExpressionEvaluator(String columnName, String expression) throws Exception {
    TransformExpressionTree expressionTree;
    expressionTree = COMPILER_CACHE.get().compileToExpressionTree(expression);
    _rootNode = planExecution(expressionTree);
  }

  private ExecutableNode planExecution(TransformExpressionTree expressionTree) throws Exception {
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
        childNode = new ColumnExecutionNode(childExpression.getValue());
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

    FunctionInfo functionInfo = FunctionRegistry.resolve(transformName, argumentTypes);
    return new FunctionExecutionNode(functionInfo, childNodes);
  }

  public Object evaluate(GenericRow row) {
    return _rootNode.execute(row);
  }

  private static interface ExecutableNode {
    /**
     * 
     * @param row
     * @return
     */
    Object execute(GenericRow row);

    /**
     * 
     * @return
     */
    Class<?> getReturnType();

  }

  private static class FunctionExecutionNode implements ExecutableNode {
    FunctionInvoker _functionInvoker;
    ExecutableNode[] _argumentProviders;
    Object[] _argInputs;

    public FunctionExecutionNode(FunctionInfo functionInfo, ExecutableNode[] argumentProviders) throws Exception {
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
    private String _value;

    public ConstantExecutionNode(String value) {
      _value = value;
    }

    @Override
    public Object execute(GenericRow row) {
      return _value;
    }

    @Override
    public Class<?> getReturnType() {
      return String.class;
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
