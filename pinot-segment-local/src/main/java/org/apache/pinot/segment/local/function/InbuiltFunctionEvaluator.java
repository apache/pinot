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
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
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
    _rootNode = planExecution(RequestContextUtils.getExpression(functionExpression));
  }

  private ExecutableNode planExecution(ExpressionContext expression) {
    switch (expression.getType()) {
      case LITERAL:
        // TODO: pass literal with type into ConstantExecutionNode.
        return new ConstantExecutionNode(expression.getLiteral().getStringValue());
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
        switch (functionName) {
          case "and":
            return new AndExecutionNode(childNodes);
          case "or":
            return new OrExecutionNode(childNodes);
          case "not":
            Preconditions.checkState(numArguments == 1, "NOT function expects 1 argument, got: %s", numArguments);
            return new NotExecutionNode(childNodes[0]);
          case "arrayvalueconstructor":
            Object[] values = new Object[numArguments];
            int i = 0;
            for (ExpressionContext literal : arguments) {
              values[i++] = literal.getLiteral().getValue();
            }
            return new ArrayConstantExecutionNode(values);
          default:
            FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName, numArguments);
            if (functionInfo == null) {
              if (FunctionRegistry.containsFunction(functionName)) {
                throw new IllegalStateException(
                    String.format("Unsupported function: %s with %d parameters", functionName, numArguments));
              } else {
                throw new IllegalStateException(
                    String.format("Unsupported function: %s not found", functionName));
              }
            }
            return new FunctionExecutionNode(functionInfo, childNodes);
        }
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

  private static class NotExecutionNode implements ExecutableNode {
    private final ExecutableNode _argumentNode;

    NotExecutionNode(ExecutableNode argumentNode) {
      _argumentNode = argumentNode;
    }

    @Override
    public Object execute(GenericRow row) {
      return !((Boolean) _argumentNode.execute(row));
    }

    @Override
    public Object execute(Object[] values) {
      return !((Boolean) _argumentNode.execute(values));
    }
  }

  private static class OrExecutionNode implements ExecutableNode {
    private final ExecutableNode[] _argumentNodes;

    OrExecutionNode(ExecutableNode[] argumentNodes) {
      _argumentNodes = argumentNodes;
    }

    @Override
    public Object execute(GenericRow row) {
      for (ExecutableNode executableNode : _argumentNodes) {
        Boolean res = (Boolean) executableNode.execute(row);
        if (res) {
          return true;
        }
      }
      return false;
    }

    @Override
    public Object execute(Object[] values) {
      for (ExecutableNode executableNode : _argumentNodes) {
        Boolean res = (Boolean) executableNode.execute(values);
        if (res) {
          return true;
        }
      }
      return false;
    }
  }

  private static class AndExecutionNode implements ExecutableNode {
    private final ExecutableNode[] _argumentNodes;

    AndExecutionNode(ExecutableNode[] argumentNodes) {
      _argumentNodes = argumentNodes;
    }

    @Override
    public Object execute(GenericRow row) {
      for (ExecutableNode executableNode : _argumentNodes) {
        Boolean res = (Boolean) executableNode.execute(row);
        if (!res) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Object execute(Object[] values) {
      for (ExecutableNode executableNode : _argumentNodes) {
        Boolean res = (Boolean) executableNode.execute(values);
        if (!res) {
          return false;
        }
      }
      return true;
    }
  }

  private static class FunctionExecutionNode implements ExecutableNode {
    final FunctionInvoker _functionInvoker;
    final FunctionInfo _functionInfo;
    final ExecutableNode[] _argumentNodes;
    final Object[] _arguments;

    FunctionExecutionNode(FunctionInfo functionInfo, ExecutableNode[] argumentNodes) {
      _functionInvoker = new FunctionInvoker(functionInfo);
      _functionInfo = functionInfo;
      _argumentNodes = argumentNodes;
      _arguments = new Object[_argumentNodes.length];
    }

    @Override
    public Object execute(GenericRow row) {
      try {
        int numArguments = _argumentNodes.length;
        for (int i = 0; i < numArguments; i++) {
          _arguments[i] = _argumentNodes[i].execute(row);
        }
        if (!_functionInfo.hasNullableParameters()) {
          // Preserve null values during ingestion transformation if function is an inbuilt
          // scalar function that cannot handle nulls, and invoked with null parameter(s).
          for (Object argument : _arguments) {
            if (argument == null) {
              return null;
            }
          }
        }
        if (_functionInvoker.getMethod().isVarArgs()) {
          return _functionInvoker.invoke(new Object[]{_arguments});
        }
        _functionInvoker.convertTypes(_arguments);
        return _functionInvoker.invoke(_arguments);
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while executing function: " + this, e);
      }
    }

    @Override
    public Object execute(Object[] values) {
      try {
        int numArguments = _argumentNodes.length;
        for (int i = 0; i < numArguments; i++) {
          _arguments[i] = _argumentNodes[i].execute(values);
        }
        if (!_functionInfo.hasNullableParameters()) {
          // Preserve null values during ingestion transformation if function is an inbuilt
          // scalar function that cannot handle nulls, and invoked with null parameter(s).
          for (Object argument : _arguments) {
            if (argument == null) {
              return null;
            }
          }
        }
        if (_functionInvoker.getMethod().isVarArgs()) {
          return _functionInvoker.invoke(new Object[]{_arguments});
        }
        _functionInvoker.convertTypes(_arguments);
        return _functionInvoker.invoke(_arguments);
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while executing function: " + this, e);
      }
    }

    @Override
    public String toString() {
      return _functionInvoker.getMethod().getName() + '(' + StringUtils.join(_argumentNodes, ',') + ')';
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

    @Override
    public String toString() {
      return String.format("'%s'", _value);
    }
  }

  private static class ArrayConstantExecutionNode implements ExecutableNode {
    final Object[] _value;

    ArrayConstantExecutionNode(Object[] value) {
      _value = value;
    }

    @Override
    public Object[] execute(GenericRow row) {
      return _value;
    }

    @Override
    public Object[] execute(Object[] values) {
      return _value;
    }

    @Override
    public String toString() {
      return String.format("'%s'", Arrays.toString(_value));
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

    @Override
    public String toString() {
      return _column;
    }
  }
}
