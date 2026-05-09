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
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.spi.function.ExecutableFunctionEvaluator;
import org.apache.pinot.spi.data.readers.GenericRow;

import static java.nio.charset.StandardCharsets.UTF_8;


/// A [org.apache.pinot.spi.function.FunctionEvaluator] for partition expressions backed by the Pinot
/// [FunctionRegistry].
///
/// This evaluator is structurally equivalent to [InbuiltFunctionEvaluator] with one critical difference:
/// when a `String` value is passed to a function that expects `byte[]`, the conversion uses
/// *UTF-8 encoding* rather than hex-decoding. This matches ingestion semantics where user-facing string column
/// values are treated as text, not as hex-encoded binary data.
///
/// Example: `murmur2(lower(raw_key))` with input `"HeLLo"` evaluates as follows:
///
/// - `lower("HeLLo")` -> `"hello"` (String)
/// - `murmur2("hello")` converts `"hello"` to UTF-8 bytes, then hashes
///
/// **Thread-safety:** Instances are safe for both concurrent invocation by multiple threads and
/// re-entrant invocation on a single thread. Each [PartitionFunctionExecutionNode] allocates a fresh
/// argument scratch array per `execute` call.
public final class PartitionFunctionEvaluator extends ExecutableFunctionEvaluator {

  public PartitionFunctionEvaluator(String functionExpression) {
    this(functionExpression, new ArrayList<>());
  }

  /// Two-phase constructor: `planExecution` is evaluated first (left-to-right argument evaluation in Java),
  /// populating `arguments` as a side effect, so the completed list is ready when passed to `super()`.
  private PartitionFunctionEvaluator(String functionExpression, List<String> arguments) {
    super(planExecution(RequestContextUtils.getExpression(functionExpression), arguments), arguments,
        functionExpression);
  }

  private static ExecutableNode planExecution(ExpressionContext expression, List<String> arguments) {
    switch (expression.getType()) {
      case LITERAL:
        return new ConstantNode(expression.getLiteral().getValue());
      case IDENTIFIER:
        String columnName = expression.getIdentifier();
        ColumnNode columnNode = new ColumnNode(columnName, arguments.size());
        arguments.add(columnName);
        return columnNode;
      case FUNCTION:
        FunctionContext function = expression.getFunction();
        List<ExpressionContext> args = function.getArguments();
        int numArguments = args.size();
        ExecutableNode[] childNodes = new ExecutableNode[numArguments];
        for (int i = 0; i < numArguments; i++) {
          childNodes[i] = planExecution(args.get(i), arguments);
        }
        String functionName = function.getFunctionName();
        String canonicalName = FunctionRegistry.canonicalize(functionName);
        switch (canonicalName) {
          case "and":
            return new AndNode(childNodes);
          case "or":
            return new OrNode(childNodes);
          case "not":
            Preconditions.checkState(numArguments == 1, "NOT function expects 1 argument, got: %s", numArguments);
            return new NotNode(childNodes[0]);
          case "arrayvalueconstructor":
            Object[] values = new Object[numArguments];
            int i = 0;
            for (ExpressionContext literal : args) {
              values[i++] = literal.getLiteral().getValue();
            }
            return new ArrayConstantNode(values);
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
            return new PartitionFunctionExecutionNode(functionInfo, childNodes);
        }
      default:
        throw new IllegalStateException();
    }
  }

  /// Executes a Pinot-registry function via [FunctionInvoker], with null propagation and type conversion.
  /// Unlike [InbuiltFunctionEvaluator.FunctionExecutionNode], this node converts `String` to
  /// `byte[]` using UTF-8 encoding instead of hex-decoding, matching ingestion semantics for partition
  /// expressions.
  ///
  /// Allocates a fresh argument scratch array per `execute` call so that concurrent invocations from
  /// multiple threads, and re-entrant invocations on a single thread, do not share mutable state.
  private static class PartitionFunctionExecutionNode implements ExecutableNode {
    private final FunctionInvoker _functionInvoker;
    private final FunctionInfo _functionInfo;
    private final ExecutableNode[] _argumentNodes;

    PartitionFunctionExecutionNode(FunctionInfo functionInfo, ExecutableNode[] argumentNodes) {
      _functionInvoker = new FunctionInvoker(functionInfo);
      _functionInfo = functionInfo;
      _argumentNodes = argumentNodes;
    }

    @Override
    public Object execute(GenericRow row) {
      try {
        int numArguments = _argumentNodes.length;
        Object[] arguments = new Object[numArguments];
        for (int i = 0; i < numArguments; i++) {
          arguments[i] = _argumentNodes[i].execute(row);
        }
        return invokeWithPartitionConversion(arguments);
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while executing function: " + this + ": " + e.getMessage(), e);
      }
    }

    @Override
    public Object execute(Object[] values) {
      try {
        int numArguments = _argumentNodes.length;
        Object[] arguments = new Object[numArguments];
        for (int i = 0; i < numArguments; i++) {
          arguments[i] = _argumentNodes[i].execute(values);
        }
        return invokeWithPartitionConversion(arguments);
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while executing function: " + this + ": " + e.getMessage(), e);
      }
    }

    private Object invokeWithPartitionConversion(Object[] arguments) {
      if (!_functionInfo.hasNullableParameters()) {
        for (Object argument : arguments) {
          if (argument == null) {
            return null;
          }
        }
      }
      if (_functionInvoker.getMethod().isVarArgs()) {
        return _functionInvoker.invoke(new Object[]{arguments});
      }
      convertTypesWithUtf8StringToBytes(arguments);
      return _functionInvoker.invoke(arguments);
    }

    /// Converts argument types to match the parameter classes, using UTF-8 encoding for `String`->`byte[]`
    /// conversions (rather than hex-decoding used by the standard [FunctionInvoker#convertTypes]).
    private void convertTypesWithUtf8StringToBytes(Object[] arguments) {
      Class<?>[] parameterClasses = _functionInvoker.getParameterClasses();
      PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
      int numParameters = parameterClasses.length;
      for (int i = 0; i < numParameters; i++) {
        Object argument = arguments[i];
        if (argument == null) {
          continue;
        }
        Class<?> parameterClass = parameterClasses[i];
        Class<?> argumentClass = argument.getClass();
        if (parameterClass.isAssignableFrom(argumentClass)) {
          continue;
        }
        // Special case: convert String to byte[] using UTF-8 (ingestion semantics)
        if (parameterClass == byte[].class && argument instanceof String) {
          arguments[i] = ((String) argument).getBytes(UTF_8);
          continue;
        }
        PinotDataType parameterType = parameterTypes[i];
        PinotDataType argumentType = FunctionUtils.getArgumentType(argument);
        Preconditions.checkArgument(parameterType != null && argumentType != null,
            "Cannot convert value from class: %s to class: %s", argumentClass, parameterClass);
        arguments[i] = parameterType.convert(argument, argumentType);
      }
    }

    @Override
    public String toString() {
      return _functionInvoker.getMethod().getName() + '(' + StringUtils.join(_argumentNodes, ',') + ')';
    }
  }
}
