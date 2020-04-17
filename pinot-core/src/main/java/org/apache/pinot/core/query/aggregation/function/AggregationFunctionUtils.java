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
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;
import org.apache.pinot.parsers.CompilerConstants;


/**
 * The <code>AggregationFunctionUtils</code> class provides utility methods for aggregation function.
 */
public class AggregationFunctionUtils {
  private AggregationFunctionUtils() {
  }

  /**
   * Extracts the aggregation column (could be column name or UDF expression) from the {@link AggregationInfo}.
   *
   *
   */
  /**
   * Returns the arguments for {@link AggregationFunction} as List of Strings.
   * For backward compatibility, it uses the new Thrift field aggregationFunctionArgs if found, or else,
   * falls back to the previous aggregationParams based approach.
   *
   * @param aggregationInfo Aggregation Info
   * @return List of aggregation function arguments
   */
  public static List<String> getAggregationArgs(AggregationInfo aggregationInfo) {
    List<String> aggregationFunctionArgs = aggregationInfo.getAggregationFunctionArgs();
    if (aggregationFunctionArgs != null) {
      return aggregationFunctionArgs;
    }

    String params = aggregationInfo.getAggregationParams().get(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO);
    return Arrays.asList(params.split(CompilerConstants.AGGREGATION_FUNCTION_ARG_SEPARATOR));
  }

  /**
   * Creates an {@link AggregationFunctionColumnPair} from the {@link AggregationInfo}.
   * Asserts that the function only expects one argument.
   */
  public static AggregationFunctionColumnPair getFunctionColumnPair(AggregationInfo aggregationInfo) {
    List<String> aggregationArgs = getAggregationArgs(aggregationInfo);
    int numArgs = aggregationArgs.size();
    AggregationFunctionType functionType =
        AggregationFunctionType.getAggregationFunctionType(aggregationInfo.getAggregationType());
    Preconditions.checkState(numArgs == 1, "Expected one argument for '" + functionType + "', got: " + numArgs);
    return new AggregationFunctionColumnPair(functionType, aggregationArgs.get(0));
  }

  public static boolean isDistinct(AggregationFunctionContext[] functionContexts) {
    return functionContexts.length == 1
        && functionContexts[0].getAggregationFunction().getType() == AggregationFunctionType.DISTINCT;
  }

  /**
   * Creates an {@link AggregationFunctionContext} from the {@link AggregationInfo}.
   * NOTE: This method does not work for {@code DISTINCT} aggregation function.
   * TODO: Remove this method and always pass in the broker request
   */
  public static AggregationFunctionContext getAggregationFunctionContext(AggregationInfo aggregationInfo) {
    return getAggregationFunctionContext(aggregationInfo, null);
  }

  /**
   * NOTE: Broker request cannot be {@code null} for {@code DISTINCT} aggregation function.
   * TODO: Always pass in non-null broker request
   */
  public static AggregationFunctionContext getAggregationFunctionContext(AggregationInfo aggregationInfo,
      @Nullable BrokerRequest brokerRequest) {
    List<String> aggregationArgs = getAggregationArgs(aggregationInfo);
    AggregationFunction aggregationFunction =
        AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    return new AggregationFunctionContext(aggregationFunction, aggregationArgs);
  }

  public static AggregationFunctionContext[] getAggregationFunctionContexts(BrokerRequest brokerRequest) {
    List<AggregationInfo> aggregationInfos = brokerRequest.getAggregationsInfo();
    int numAggregationFunctions = aggregationInfos.size();
    AggregationFunctionContext[] aggregationFunctionContexts = new AggregationFunctionContext[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationInfo aggregationInfo = aggregationInfos.get(i);
      aggregationFunctionContexts[i] = getAggregationFunctionContext(aggregationInfo, brokerRequest);
    }
    return aggregationFunctionContexts;
  }

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
   * Asserts that percentile value is within 0 and 100.
   *
   * @param percentileString Input String
   * @return Percentile value parsed from String.
   */
  public static int parsePercentile(String percentileString) {
    int percentile = Integer.parseInt(percentileString);
    Preconditions.checkState(percentile >= 0 && percentile <= 100);
    return percentile;
  }

  /**
   * Helper function to concatenate arguments using separator.
   *
   * @param arguments Arguments to concatenate
   * @return Concatenated String of arguments
   */
  public static String concatArgs(List<String> arguments) {
    return (arguments.size() > 1) ? String.join(CompilerConstants.AGGREGATION_FUNCTION_ARG_SEPARATOR, arguments)
        : arguments.get(0);
  }
}
