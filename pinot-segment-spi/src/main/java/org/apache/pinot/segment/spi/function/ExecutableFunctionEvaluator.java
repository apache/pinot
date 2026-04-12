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
package org.apache.pinot.segment.spi.function;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Shared runtime implementation for expression evaluators backed by an executable node tree.
 *
 * <p>The planner for a concrete evaluator is responsible for binding scalar functions and constructing the root
 * {@link ExecutableNode}. This class owns the repeated row/value execution logic so ingestion and partition paths do
 * not maintain separate node evaluators.
 */
public class ExecutableFunctionEvaluator implements FunctionEvaluator {
  /**
   * Invokes a bound function with already-evaluated child arguments.
   */
  public interface Invoker {
    Object invoke(Object[] arguments);
  }

  /**
   * Executes one node within the evaluator tree.
   */
  public interface ExecutableNode {
    Object execute(GenericRow row);

    Object execute(Object[] values);
  }

  private final ExecutableNode _rootNode;
  private final List<String> _arguments;
  private final String _expression;

  public ExecutableFunctionEvaluator(ExecutableNode rootNode, List<String> arguments, String expression) {
    _rootNode = Preconditions.checkNotNull(rootNode, "Executable root node must be configured");
    _arguments = List.copyOf(Preconditions.checkNotNull(arguments, "Evaluator arguments must be configured"));
    _expression = Preconditions.checkNotNull(expression, "Evaluator expression must be configured");
  }

  @Override
  public List<String> getArguments() {
    return _arguments;
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    return _rootNode.execute(genericRow);
  }

  @Override
  public Object evaluate(Object[] values) {
    return _rootNode.execute(values);
  }

  @Override
  public String toString() {
    return _expression;
  }

  /**
   * Returns a constant value.
   */
  public static class ConstantNode implements ExecutableNode {
    private final Object _value;

    public ConstantNode(Object value) {
      _value = value;
    }

    @Override
    public Object execute(GenericRow row) {
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

  /**
   * Returns a constant array value.
   */
  public static class ArrayConstantNode implements ExecutableNode {
    private final Object[] _value;

    public ArrayConstantNode(Object[] value) {
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

  /**
   * Reads one referenced argument from a row or positional argument array.
   */
  public static class ColumnNode implements ExecutableNode {
    private final String _column;
    private final int _id;

    public ColumnNode(String column, int id) {
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

  /**
   * Executes a bound function after evaluating all child arguments.
   */
  public static class FunctionNode implements ExecutableNode {
    private final String _functionName;
    private final Invoker _invoker;
    private final ExecutableNode[] _argumentNodes;
    private final ThreadLocal<Object[]> _arguments;

    public FunctionNode(String functionName, Invoker invoker, ExecutableNode[] argumentNodes) {
      _functionName = functionName;
      _invoker = invoker;
      _argumentNodes = argumentNodes;
      _arguments = ThreadLocal.withInitial(() -> new Object[argumentNodes.length]);
    }

    @Override
    public Object execute(GenericRow row) {
      Object[] arguments = _arguments.get();
      for (int i = 0; i < _argumentNodes.length; i++) {
        arguments[i] = _argumentNodes[i].execute(row);
      }
      return _invoker.invoke(arguments);
    }

    @Override
    public Object execute(Object[] values) {
      Object[] arguments = _arguments.get();
      for (int i = 0; i < _argumentNodes.length; i++) {
        arguments[i] = _argumentNodes[i].execute(values);
      }
      return _invoker.invoke(arguments);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder(_functionName).append('(');
      for (int i = 0; i < _argumentNodes.length; i++) {
        if (i > 0) {
          builder.append(',');
        }
        builder.append(_argumentNodes[i]);
      }
      return builder.append(')').toString();
    }
  }

  /**
   * Three-valued logical NOT.
   */
  public static class NotNode implements ExecutableNode {
    private final ExecutableNode _argumentNode;

    public NotNode(ExecutableNode argumentNode) {
      _argumentNode = argumentNode;
    }

    @Override
    public Object execute(GenericRow row) {
      Boolean result = (Boolean) _argumentNode.execute(row);
      return result != null ? !result : null;
    }

    @Override
    public Object execute(Object[] values) {
      Boolean result = (Boolean) _argumentNode.execute(values);
      return result != null ? !result : null;
    }
  }

  /**
   * Three-valued logical OR with short-circuit evaluation.
   */
  public static class OrNode implements ExecutableNode {
    private final ExecutableNode[] _argumentNodes;

    public OrNode(ExecutableNode[] argumentNodes) {
      _argumentNodes = argumentNodes;
    }

    @Override
    public Object execute(GenericRow row) {
      boolean hasNull = false;
      for (ExecutableNode argumentNode : _argumentNodes) {
        Boolean result = (Boolean) argumentNode.execute(row);
        if (result == null) {
          hasNull = true;
        } else if (result) {
          return true;
        }
      }
      return hasNull ? null : false;
    }

    @Override
    public Object execute(Object[] values) {
      boolean hasNull = false;
      for (ExecutableNode argumentNode : _argumentNodes) {
        Boolean result = (Boolean) argumentNode.execute(values);
        if (result == null) {
          hasNull = true;
        } else if (result) {
          return true;
        }
      }
      return hasNull ? null : false;
    }
  }

  /**
   * Three-valued logical AND with short-circuit evaluation.
   */
  public static class AndNode implements ExecutableNode {
    private final ExecutableNode[] _argumentNodes;

    public AndNode(ExecutableNode[] argumentNodes) {
      _argumentNodes = argumentNodes;
    }

    @Override
    public Object execute(GenericRow row) {
      boolean hasNull = false;
      for (ExecutableNode argumentNode : _argumentNodes) {
        Boolean result = (Boolean) argumentNode.execute(row);
        if (result == null) {
          hasNull = true;
        } else if (!result) {
          return false;
        }
      }
      return hasNull ? null : true;
    }

    @Override
    public Object execute(Object[] values) {
      boolean hasNull = false;
      for (ExecutableNode argumentNode : _argumentNodes) {
        Boolean result = (Boolean) argumentNode.execute(values);
        if (result == null) {
          hasNull = true;
        } else if (!result) {
          return false;
        }
      }
      return hasNull ? null : true;
    }
  }
}
