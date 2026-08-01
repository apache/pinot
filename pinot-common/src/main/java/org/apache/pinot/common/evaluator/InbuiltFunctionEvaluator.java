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
package org.apache.pinot.common.evaluator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.common.utils.VariantUtils.ResultType;
import org.apache.pinot.common.utils.VariantUtils.ReusableResult;
import org.apache.pinot.common.utils.VariantUtils.VariantPath;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.PinotDataType;


/// Evaluates an expression.
///
/// This is optimized for evaluating an expression multiple times with different inputs.
///
/// Overall idea: parse the expression into an ExecutableNode, where an ExecutableNode can be:
///
/// - FunctionNode - executes a function
/// - ColumnNode - fetches the value of the column from the input GenericRow
/// - ConstantNode - returns the literal value
///
/// NOTE: This class is not thread safe. Function nodes refill one reusable argument array on every evaluation, so
/// two threads evaluating the same instance can read each other's argument values. Give each thread its own instance.
@NotThreadSafe
public class InbuiltFunctionEvaluator implements FunctionEvaluator {
  // Root of the execution tree
  private final ExecutableNode _rootNode;
  private final List<String> _arguments;
  private final String _functionExpression;

  public InbuiltFunctionEvaluator(String functionExpression) {
    _functionExpression = functionExpression;
    _arguments = new ArrayList<>();
    _rootNode = planExecution(RequestContextUtils.getExpression(functionExpression));
  }

  private ExecutableNode planExecution(ExpressionContext expression) {
    switch (expression.getType()) {
      case LITERAL:
        return new ConstantExecutionNode(expression.getLiteral().getValue());
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
        String canonicalName = FunctionRegistry.canonicalize(functionName);
        switch (canonicalName) {
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
            FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(canonicalName, numArguments);
            if (functionInfo == null) {
              if (FunctionRegistry.contains(canonicalName)) {
                throw new IllegalStateException(
                    String.format("Unsupported function: %s with %d arguments", functionName, numArguments));
              } else {
                throw new IllegalStateException(String.format("Unsupported function: %s", functionName));
              }
            }
            ExecutableNode variantExecutionNode =
                VariantExecutionNode.tryCreate(functionName, canonicalName, arguments, childNodes);
            if (variantExecutionNode != null) {
              return variantExecutionNode;
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

  @Override
  public String toString() {
    return _functionExpression;
  }

  /// Planned ingestion evaluator for Variant scalar functions with literal path and target-type operands.
  ///
  /// <p>Each node compiles its literals once and owns a reusable cursor result. Like the enclosing evaluator, instances
  /// are intended to be confined to the record-transformer thread and are not thread-safe.
  private static class VariantExecutionNode implements ExecutableNode {
    private static final String VARIANT_GET = "variantget";
    private static final String TRY_VARIANT_GET = "tryvariantget";
    private static final String VARIANT_EXISTS = "variantexists";
    private static final String IS_VARIANT_NULL = "isvariantnull";
    private static final String VARIANT_TYPE_OF = "varianttypeof";

    private final String _functionName;
    private final ExecutableNode _variantNode;
    private final VariantOperation _operation;
    private final VariantPath _path;
    @Nullable
    private final ResultType _targetType;
    private final ReusableResult _reusableResult = new ReusableResult();

    private VariantExecutionNode(String functionName, ExecutableNode variantNode, VariantOperation operation,
        VariantPath path, @Nullable ResultType targetType) {
      _functionName = functionName;
      _variantNode = variantNode;
      _operation = operation;
      _path = path;
      _targetType = targetType;
    }

    @Nullable
    static ExecutableNode tryCreate(String functionName, String canonicalName, List<ExpressionContext> arguments,
        ExecutableNode[] childNodes) {
      int numArguments = arguments.size();
      switch (canonicalName) {
        case VARIANT_GET:
        case TRY_VARIANT_GET:
          if ((numArguments != 2 && numArguments != 3) || !hasStringLiteral(arguments, 1)
              || (numArguments == 3 && !hasStringLiteral(arguments, 2))) {
            return null;
          }
          return new VariantExecutionNode(functionName, childNodes[0],
              canonicalName.equals(VARIANT_GET) ? VariantOperation.GET : VariantOperation.TRY_GET,
              VariantUtils.compilePath(stringLiteral(arguments, 1)),
              numArguments == 2 ? ResultType.VARIANT
                  : VariantUtils.parseResultType(stringLiteral(arguments, 2)));
        case VARIANT_EXISTS:
          if (numArguments != 2 || !hasStringLiteral(arguments, 1)) {
            return null;
          }
          return new VariantExecutionNode(functionName, childNodes[0], VariantOperation.EXISTS,
              VariantUtils.compilePath(stringLiteral(arguments, 1)), null);
        case IS_VARIANT_NULL:
        case VARIANT_TYPE_OF:
          if ((numArguments != 1 && numArguments != 2)
              || (numArguments == 2 && !hasStringLiteral(arguments, 1))) {
            return null;
          }
          return new VariantExecutionNode(functionName, childNodes[0],
              canonicalName.equals(IS_VARIANT_NULL) ? VariantOperation.IS_NULL : VariantOperation.TYPE_OF,
              VariantUtils.compilePath(numArguments == 1 ? "$" : stringLiteral(arguments, 1)), null);
        default:
          return null;
      }
    }

    private static boolean hasStringLiteral(List<ExpressionContext> arguments, int index) {
      ExpressionContext argument = arguments.get(index);
      return argument.getType() == ExpressionContext.Type.LITERAL
          && argument.getLiteral().getValue() instanceof String;
    }

    private static String stringLiteral(List<ExpressionContext> arguments, int index) {
      return (String) arguments.get(index).getLiteral().getValue();
    }

    @Override
    public Object execute(GenericRow row) {
      return executeVariant(_variantNode.execute(row));
    }

    @Override
    public Object execute(Object[] values) {
      return executeVariant(_variantNode.execute(values));
    }

    @Nullable
    private Object executeVariant(@Nullable Object input) {
      try {
        byte[] variant = toBytes(input);
        switch (_operation) {
          case GET:
            return extract(variant, false);
          case TRY_GET:
            return extract(variant, true);
          case EXISTS:
            return VariantUtils.variantExists(variant, _path, _reusableResult);
          case IS_NULL:
            return VariantUtils.isVariantNull(variant, _path, _reusableResult);
          case TYPE_OF:
            return VariantUtils.variantTypeOf(variant, _path, _reusableResult);
          default:
            throw new IllegalStateException("Unhandled Variant operation: " + _operation);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Caught exception while executing function: " + _functionName + ": " + e.getMessage(), e);
      }
    }

    @Nullable
    private Object extract(@Nullable byte[] variant, boolean tolerant) {
      ResultType targetType = Preconditions.checkNotNull(_targetType, "Variant target type must be planned");
      boolean present = tolerant
          ? VariantUtils.tryExtractInto(variant, _path, targetType, _reusableResult)
          : VariantUtils.extractInto(variant, _path, targetType, _reusableResult);
      if (!present) {
        return null;
      }
      return _reusableResult.getExternalValue(targetType);
    }

    @Nullable
    private static byte[] toBytes(@Nullable Object input) {
      if (input == null) {
        return null;
      }
      if (input instanceof byte[]) {
        return (byte[]) input;
      }
      return (byte[]) PinotDataType.BYTES.convert(input, FunctionUtils.getArgumentType(input));
    }
  }

  private enum VariantOperation {
    GET,
    TRY_GET,
    EXISTS,
    IS_NULL,
    TYPE_OF
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
      Boolean res = (Boolean) _argumentNode.execute(row);
      if (res == null) {
        return null;
      } else {
        return !res;
      }
    }

    @Override
    public Object execute(Object[] values) {
      Boolean res = (Boolean) _argumentNode.execute(values);
      if (res == null) {
        return null;
      } else {
        return !res;
      }
    }
  }

  private static class OrExecutionNode implements ExecutableNode {
    private final ExecutableNode[] _argumentNodes;

    OrExecutionNode(ExecutableNode[] argumentNodes) {
      _argumentNodes = argumentNodes;
    }

    @Override
    public Object execute(GenericRow row) {
      boolean hasNull = false;

      for (ExecutableNode executableNode : _argumentNodes) {
        Boolean res = (Boolean) executableNode.execute(row);
        if (res == null) {
          hasNull = true;
          continue;
        }
        if (res) {
          return true;
        }
      }

      return hasNull ? null : false;
    }

    @Override
    public Object execute(Object[] values) {
      boolean hasNull = false;

      for (ExecutableNode executableNode : _argumentNodes) {
        Boolean res = (Boolean) executableNode.execute(values);
        if (res == null) {
          hasNull = true;
          continue;
        }
        if (res) {
          return true;
        }
      }

      return hasNull ? null : false;
    }
  }

  private static class AndExecutionNode implements ExecutableNode {
    private final ExecutableNode[] _argumentNodes;

    AndExecutionNode(ExecutableNode[] argumentNodes) {
      _argumentNodes = argumentNodes;
    }

    @Override
    public Object execute(GenericRow row) {
      boolean hasNull = false;

      for (ExecutableNode executableNode : _argumentNodes) {
        Boolean res = (Boolean) executableNode.execute(row);
        if (res == null) {
          hasNull = true;
          continue;
        }
        if (!res) {
          return false;
        }
      }

      return hasNull ? null : true;
    }

    @Override
    public Object execute(Object[] values) {
      boolean hasNull = false;

      for (ExecutableNode executableNode : _argumentNodes) {
        Boolean res = (Boolean) executableNode.execute(values);
        if (res == null) {
          hasNull = true;
          continue;
        }
        if (!res) {
          return false;
        }
      }

      return hasNull ? null : true;
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
        throw new RuntimeException("Caught exception while executing function: " + this + ": " + e.getMessage(), e);
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
        throw new RuntimeException("Caught exception while executing function: " + this + ": " + e.getMessage(), e);
      }
    }

    @Override
    public String toString() {
      return _functionInvoker.getMethod().getName() + '(' + StringUtils.join(_argumentNodes, ',') + ')';
    }
  }

  private static class ConstantExecutionNode implements ExecutableNode {
    final Object _value;

    ConstantExecutionNode(Object value) {
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
