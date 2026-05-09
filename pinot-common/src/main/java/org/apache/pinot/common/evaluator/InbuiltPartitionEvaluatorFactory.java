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
import java.lang.reflect.Method;
import java.util.List;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.common.function.scalar.InternalFunctions;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionEvaluatorFactory;
import org.apache.pinot.spi.function.FunctionEvaluator;


/// [PartitionEvaluatorFactory] backed by [PartitionFunctionEvaluator].
///
/// Delegates all scalar-function resolution and evaluation to Pinot's [FunctionRegistry], ensuring
/// that partition expressions use the same function semantics as ingestion transforms. String-to-bytes conversions
/// use UTF-8 encoding so that hash functions (`md5`, `murmur2`, `fnv1a_32`, etc.) operate on the
/// raw string content rather than a hex-decoded representation.
public class InbuiltPartitionEvaluatorFactory implements PartitionEvaluatorFactory {

  @Override
  public FunctionEvaluator compile(String rawColumn, String expression) {
    // Validate determinism first — non-deterministic functions (now(), ago(), cid(), etc.) must be rejected before
    // we check column references, because functions like now() have zero column references.
    validateDeterministic(RequestContextUtils.getExpression(expression));
    PartitionFunctionEvaluator evaluator = new PartitionFunctionEvaluator(expression);
    List<String> args = evaluator.getArguments();
    // The expression is canonicalized to lowercase before being passed here, so compare case-insensitively.
    Preconditions.checkArgument(args.size() == 1 && rawColumn.equalsIgnoreCase(args.get(0)),
        "Partition expression for column '%s' must reference exactly that column, got: %s", rawColumn, args);
    return evaluator;
  }

  private static void validateDeterministic(ExpressionContext expr) {
    if (expr.getType() != ExpressionContext.Type.FUNCTION) {
      return;
    }
    FunctionContext function = expr.getFunction();
    String canonicalName = FunctionRegistry.canonicalize(function.getFunctionName());
    List<ExpressionContext> args = function.getArguments();
    FunctionInfo info = FunctionRegistry.lookupFunctionInfo(canonicalName, args.size());
    if (info != null) {
      if (!info.isDeterministic() || !isAllowedForPartitioning(info.getMethod())) {
        throw new IllegalArgumentException(
            "Partition scalar function '" + canonicalName + "' is not allowed because it is non-deterministic");
      }
    }
    for (ExpressionContext arg : args) {
      validateDeterministic(arg);
    }
  }

  /// Partition expressions must stay stable for ingestion and query pruning. Functions that read query-thread context
  /// or intentionally block are not safe even if the broader SQL engine exposes them as regular scalar functions.
  private static boolean isAllowedForPartitioning(Method method) {
    Class<?> declaringClass = method.getDeclaringClass();
    if (declaringClass == InternalFunctions.class) {
      return false;
    }
    if (declaringClass == DateTimeFunctions.class) {
      // Block functions that read wall-clock time: the same functionExpr compiled on different nodes or at different
      // times would produce different partition assignments, which would silently corrupt routing and pruning.
      // sleep() is also blocked here (it has a side effect).
      String name = method.getName();
      return !name.equals("sleep") && !name.equals("now") && !name.equals("ago") && !name.equals("agoMV");
    }
    return true;
  }
}
