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
package org.apache.pinot.common.partition.function;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.evaluator.InbuiltFunctionEvaluator;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;


/// [PartitionFunction] that evaluates a scalar-function expression to produce a raw partition value.
///
/// The expression is configured with `functionConfig.partitionExpression` and can only reference the partition column
/// configured in the table's `columnPartitionMap`. The expression must return a numeric value, and this
/// function applies the configured [PartitionIdNormalizer] (default [PartitionIdNormalizer#POSITIVE_MODULO]) to map it
/// into `[0, numPartitions)`.
///
/// Runtime evaluation errors return `-1` so callers can treat the partition as unknown.
@SuppressWarnings("serial")
public class CustomPartitionFunction implements PartitionFunction {
  public static final String NAME = "Custom";
  public static final String PARTITION_EXPRESSION_CONFIG_KEY = "partitionExpression";
  private static final String PARTITION_ID_NORMALIZER_KEY = "partitionIdNormalizer";

  private static final PartitionIdNormalizer DEFAULT_NORMALIZER = PartitionIdNormalizer.POSITIVE_MODULO;
  private static final int INVALID_PARTITION_ID = -1;

  private final int _numPartitions;
  private final Map<String, String> _functionConfig;
  private final PartitionIdNormalizer _normalizer;
  private final String _partitionExpression;
  private final List<String> _arguments;
  @Nullable
  private transient volatile InbuiltFunctionEvaluator _evaluator;

  public CustomPartitionFunction(@Nullable String columnName, int numPartitions, Map<String, String> functionConfig) {
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    Preconditions.checkArgument(functionConfig != null,
        "'functionConfig' must be configured for custom partition function");
    _numPartitions = numPartitions;

    _partitionExpression = getPartitionExpression(functionConfig);
    _functionConfig = Collections.unmodifiableMap(new HashMap<>(functionConfig));
    _normalizer = normalizer(functionConfig, DEFAULT_NORMALIZER);
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(_partitionExpression);
    Preconditions.checkArgument(evaluator.isDeterministic(),
        "Custom partition function expression must be deterministic: %s", _partitionExpression);
    validatePartitionExpression(RequestContextUtils.getExpression(_partitionExpression), evaluator.getResultClass());
    _arguments = Collections.unmodifiableList(new ArrayList<>(evaluator.getArguments()));
    validatePartitionColumnReferences(_arguments, getPartitionColumn(columnName));
    _evaluator = evaluator;
  }

  @Override
  public int getPartition(String value) {
    try {
      return toPartitionId(evaluate(value));
    } catch (RuntimeException e) {
      return INVALID_PARTITION_ID;
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  public Map<String, String> getFunctionConfig() {
    return _functionConfig;
  }

  @Override
  public PartitionIdNormalizer getPartitionIdNormalizer() {
    return _normalizer;
  }

  @Override
  public String toString() {
    return _partitionExpression;
  }

  private Object evaluate(String value) {
    InbuiltFunctionEvaluator evaluator = evaluator();
    Object[] values = new Object[_arguments.size()];
    int numArguments = _arguments.size();
    for (int i = 0; i < numArguments; i++) {
      values[i] = value;
    }
    synchronized (evaluator) {
      return evaluator.evaluate(values);
    }
  }

  private InbuiltFunctionEvaluator evaluator() {
    InbuiltFunctionEvaluator evaluator = _evaluator;
    if (evaluator == null) {
      synchronized (this) {
        evaluator = _evaluator;
        if (evaluator == null) {
          evaluator = new InbuiltFunctionEvaluator(_partitionExpression);
          _evaluator = evaluator;
        }
      }
    }
    return evaluator;
  }

  private int toPartitionId(Object result) {
    Preconditions.checkArgument(result instanceof Number,
        "Partition expression result must be numeric, got: %s", result == null ? null : result.getClass().getName());
    return _normalizer.getPartitionId(((Number) result).longValue(), _numPartitions);
  }

  private static void validatePartitionColumnReferences(List<String> arguments, String partitionColumn) {
    boolean hasPartitionColumn = false;
    for (String argument : arguments) {
      Preconditions.checkArgument(partitionColumn.equalsIgnoreCase(argument),
          "Custom partition function expression can only reference partition column '%s', found: %s",
          partitionColumn, argument);
      hasPartitionColumn = true;
    }
    Preconditions.checkArgument(hasPartitionColumn,
        "Custom partition function expression must reference partition column '%s'", partitionColumn);
  }

  private static void validatePartitionExpression(ExpressionContext expression, Class<?> resultClass) {
    Preconditions.checkArgument(expression.getType() == ExpressionContext.Type.FUNCTION,
        "Custom partition function expression must be a scalar function expression: %s", expression);
    Preconditions.checkArgument(resultClass == Object.class || Number.class.isAssignableFrom(resultClass)
            || resultClass == byte.class || resultClass == short.class || resultClass == int.class
            || resultClass == long.class || resultClass == float.class || resultClass == double.class,
        "Custom partition function expression must return a numeric value, got: %s", resultClass.getName());
  }

  private static String getPartitionExpression(Map<String, String> functionConfig) {
    String partitionExpression = functionConfig.get(PARTITION_EXPRESSION_CONFIG_KEY);
    Preconditions.checkArgument(partitionExpression != null && !partitionExpression.trim().isEmpty(),
        "'%s' must be configured", PARTITION_EXPRESSION_CONFIG_KEY);
    return partitionExpression;
  }

  private static PartitionIdNormalizer normalizer(Map<String, String> functionConfig,
      PartitionIdNormalizer defaultNormalizer) {
    String rawNormalizer = functionConfig.get(PARTITION_ID_NORMALIZER_KEY);
    if (rawNormalizer == null || rawNormalizer.trim().isEmpty()) {
      return defaultNormalizer;
    }
    return PartitionIdNormalizer.fromConfigString(rawNormalizer);
  }

  private static String getPartitionColumn(@Nullable String partitionColumn) {
    Preconditions.checkArgument(partitionColumn != null && !partitionColumn.trim().isEmpty(),
        "Partition column must be provided for custom partition function");
    return partitionColumn.trim();
  }
}
