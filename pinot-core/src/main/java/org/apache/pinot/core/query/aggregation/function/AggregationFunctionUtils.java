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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;
import org.apache.pinot.parsers.CompilerConstants;


/**
 * The <code>AggregationFunctionUtils</code> class provides utility methods for aggregation function.
 */
public class AggregationFunctionUtils {
  private AggregationFunctionUtils() {
  }

  /**
   * Extracts the aggregation function arguments (could be column name, transform function or constant) from the
   * {@link AggregationInfo} as an array of Strings.
   * <p>NOTE: For backward-compatibility, uses the new Thrift field `expressions` if found, or falls back to the old
   * aggregationParams based approach.
   */
  public static List<String> getArguments(AggregationInfo aggregationInfo) {
    List<String> expressions = aggregationInfo.getExpressions();
    if (expressions != null) {
      return expressions;
    } else {
      // NOTE: When the server is upgraded before the broker, the expressions won't be set. Falls back to the old
      //       aggregationParams based approach.
      String column = aggregationInfo.getAggregationParams().get(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO);
      return Arrays.asList(column.split(CompilerConstants.AGGREGATION_FUNCTION_ARG_SEPARATOR));
    }
  }

  /**
   * Creates an array of {@link AggregationFunction}s based on the given {@link BrokerRequest}.
   */
  public static AggregationFunction[] getAggregationFunctions(BrokerRequest brokerRequest) {
    List<AggregationInfo> aggregationInfos = brokerRequest.getAggregationsInfo();
    int numAggregationFunctions = aggregationInfos.size();
    AggregationFunction[] aggregationFunctions = new AggregationFunction[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      aggregationFunctions[i] =
          AggregationFunctionFactory.getAggregationFunction(aggregationInfos.get(i), brokerRequest);
    }
    return aggregationFunctions;
  }

  /**
   * (For Star-Tree) Creates an {@link AggregationFunctionColumnPair} from the {@link AggregationFunction}. Returns
   * {@code null} if the {@link AggregationFunction} cannot be represented as an {@link AggregationFunctionColumnPair}
   * (e.g. has multiple arguments, argument is not column etc.).
   */
  @Nullable
  public static AggregationFunctionColumnPair getAggregationFunctionColumnPair(
      AggregationFunction aggregationFunction) {
    AggregationFunctionType aggregationFunctionType = aggregationFunction.getType();
    if (aggregationFunctionType == AggregationFunctionType.COUNT) {
      return AggregationFunctionColumnPair.COUNT_STAR;
    }
    //noinspection unchecked
    List<TransformExpressionTree> inputExpressions = aggregationFunction.getInputExpressions();
    if (inputExpressions.size() == 1) {
      TransformExpressionTree inputExpression = inputExpressions.get(0);
      if (inputExpression.isColumn()) {
        return new AggregationFunctionColumnPair(aggregationFunctionType, inputExpression.getValue());
      }
    }
    return null;
  }

  public static boolean[] getAggregationFunctionsSelectStatus(List<AggregationInfo> aggregationInfos) {
    int numAggregationFunctions = aggregationInfos.size();
    boolean[] aggregationFunctionsStatus = new boolean[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      aggregationFunctionsStatus[i] = aggregationInfos.get(i).isIsInSelectList();
    }
    return aggregationFunctionsStatus;
  }

  public static String formatValue(Object value) {
    if (value instanceof Double) {
      double doubleValue = (double) value;

      // NOTE: String.format() is very expensive, so avoid it for whole numbers that can fit in Long.
      //       We simply append ".00000" to long, in order to keep the existing behavior.
      if (doubleValue <= Long.MAX_VALUE && doubleValue >= Long.MIN_VALUE && DoubleMath
          .isMathematicalInteger(doubleValue)) {
        return (long) doubleValue + ".00000";
      } else {
        return String.format(Locale.US, "%1.5f", doubleValue);
      }
    } else {
      return value.toString();
    }
  }

  public static Serializable getSerializableValue(Object value) {
    if (value instanceof Number) {
      return (Number) value;
    } else {
      return value.toString();
    }
  }

  /**
   * Utility function to parse percentile value from string.
   * <p>Asserts that percentile value is within 0 and 100.
   * <p>NOTE: When percentileString is from the second argument (e.g. percentile(foo, 99), percentileTDigest(bar, 95),
   *          etc.), it might be standardized into single-quoted format.
   *
   * @param percentileString Input String
   * @return Percentile value parsed from String.
   */
  public static int parsePercentile(String percentileString) {
    int percentile;
    if (percentileString.charAt(0) == '\'') {
      percentile = Integer.parseInt(percentileString.substring(1, percentileString.length() - 1));
    } else {
      percentile = Integer.parseInt(percentileString);
    }
    Preconditions.checkState(percentile >= 0 && percentile <= 100);
    return percentile;
  }

  /**
   * Helper function to concatenate arguments using separator.
   *
   * @param arguments Arguments to concatenate
   * @return Concatenated String of arguments
   */
  public static String concatArgs(String[] arguments) {
    return arguments.length > 1 ? String.join(CompilerConstants.AGGREGATION_FUNCTION_ARG_SEPARATOR, arguments)
        : arguments[0];
  }

  /**
   * Collects all transform expressions required for aggregation/group-by queries.
   * <p>NOTE: We don't need to consider order-by columns here as the ordering is only allowed for aggregation functions
   *          or group-by expressions.
   */
  public static Set<TransformExpressionTree> collectExpressionsToTransform(AggregationFunction[] aggregationFunctions,
      @Nullable TransformExpressionTree[] groupByExpressions) {
    Set<TransformExpressionTree> expressions = new HashSet<>();
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      //noinspection unchecked
      expressions.addAll(aggregationFunction.getInputExpressions());
    }
    if (groupByExpressions != null) {
      expressions.addAll(Arrays.asList(groupByExpressions));
    }
    return expressions;
  }

  /**
   * Creates a map from expression required by the {@link AggregationFunction} to {@link BlockValSet} fetched from the
   * {@link TransformBlock}.
   */
  public static Map<TransformExpressionTree, BlockValSet> getBlockValSetMap(AggregationFunction aggregationFunction,
      TransformBlock transformBlock) {
    //noinspection unchecked
    List<TransformExpressionTree> expressions = aggregationFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }
    if (numExpressions == 1) {
      TransformExpressionTree expression = expressions.get(0);
      return Collections.singletonMap(expression, transformBlock.getBlockValueSet(expression));
    }
    Map<TransformExpressionTree, BlockValSet> blockValSetMap = new HashMap<>();
    for (TransformExpressionTree expression : expressions) {
      blockValSetMap.put(expression, transformBlock.getBlockValueSet(expression));
    }
    return blockValSetMap;
  }

  /**
   * (For Star-Tree) Creates a map from expression required by the {@link AggregationFunctionColumnPair} to
   * {@link BlockValSet} fetched from the {@link TransformBlock}.
   * <p>NOTE: We construct the map with original column name as the key but fetch BlockValSet with the aggregation
   *          function pair so that the aggregation result column name is consistent with or without star-tree.
   */
  public static Map<TransformExpressionTree, BlockValSet> getBlockValSetMap(
      AggregationFunctionColumnPair aggregationFunctionColumnPair, TransformBlock transformBlock) {
    TransformExpressionTree expression = new TransformExpressionTree(TransformExpressionTree.ExpressionType.IDENTIFIER,
        aggregationFunctionColumnPair.getColumn(), null);
    BlockValSet blockValSet = transformBlock.getBlockValueSet(aggregationFunctionColumnPair.toColumnName());
    return Collections.singletonMap(expression, blockValSet);
  }
}
