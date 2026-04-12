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
import org.apache.pinot.segment.spi.function.ExecutableFunctionEvaluator;


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
public class InbuiltFunctionEvaluator extends ExecutableFunctionEvaluator implements FunctionEvaluator {

  public InbuiltFunctionEvaluator(String functionExpression) {
    this(planExecution(functionExpression), functionExpression);
  }

  private InbuiltFunctionEvaluator(PlanResult planResult, String functionExpression) {
    super(planResult._rootNode, planResult._arguments, functionExpression);
  }

  private static PlanResult planExecution(String functionExpression) {
    List<String> arguments = new ArrayList<>();
    ExecutableNode rootNode = planExecution(RequestContextUtils.getExpression(functionExpression), arguments);
    return new PlanResult(rootNode, arguments);
  }

  private static ExecutableNode planExecution(ExpressionContext expression, List<String> arguments) {
    switch (expression.getType()) {
      case LITERAL:
        return new ConstantNode(expression.getLiteral().getValue());
      case IDENTIFIER:
        String columnName = expression.getIdentifier();
        ColumnNode columnExecutionNode = new ColumnNode(columnName, arguments.size());
        arguments.add(columnName);
        return columnExecutionNode;
      case FUNCTION:
        FunctionContext function = expression.getFunction();
        List<ExpressionContext> functionArguments = function.getArguments();
        int numArguments = functionArguments.size();
        ExecutableNode[] childNodes = new ExecutableNode[numArguments];
        for (int i = 0; i < numArguments; i++) {
          childNodes[i] = planExecution(functionArguments.get(i), arguments);
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
            return new FunctionNode(functionName, functionArgumentsArray -> functionArgumentsArray.clone(), childNodes);
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
            return new FunctionNode(functionName, new ScalarFunctionInvoker(functionInfo), childNodes);
        }
      default:
        throw new IllegalStateException();
    }
  }

  private static final class PlanResult {
    private final ExecutableNode _rootNode;
    private final List<String> _arguments;

    private PlanResult(ExecutableNode rootNode, List<String> arguments) {
      _rootNode = rootNode;
      _arguments = arguments;
    }
  }

  private static final class ScalarFunctionInvoker implements Invoker {
    private final FunctionInfo _functionInfo;
    private final FunctionInvoker _functionInvoker;

    private ScalarFunctionInvoker(FunctionInfo functionInfo) {
      _functionInfo = functionInfo;
      _functionInvoker = new FunctionInvoker(functionInfo);
    }

    @Override
    public Object invoke(Object[] arguments) {
      try {
        if (!_functionInfo.hasNullableParameters()) {
          // Preserve null values during ingestion transformation if function is an inbuilt scalar
          // function that cannot handle nulls, and invoked with null parameter(s).
          for (Object argument : arguments) {
            if (argument == null) {
              return null;
            }
          }
        }
        if (_functionInvoker.getMethod().isVarArgs()) {
          return _functionInvoker.invoke(new Object[]{arguments});
        }
        _functionInvoker.convertTypes(arguments);
        return _functionInvoker.invoke(arguments);
      } catch (Exception e) {
        throw new RuntimeException(
            "Caught exception while executing function: " + _functionInvoker.getMethod().getName() + ": "
                + e.getMessage(), e);
      }
    }
  }
}
