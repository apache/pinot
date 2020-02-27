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

import com.google.common.math.DoubleMath;
import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;


/**
 * The <code>AggregationFunctionUtils</code> class provides utility methods for aggregation function.
 */
public class AggregationFunctionUtils {
  private AggregationFunctionUtils() {
  }

  public static final String COLUMN_KEY = FunctionCallAstNode.COLUMN_KEY_IN_AGGREGATION_INFO;

  /**
   * Extracts the aggregation column (could be column name or UDF expression) from the {@link AggregationInfo}.
   */
  public static String getColumn(AggregationInfo aggregationInfo) {
    return aggregationInfo.getAggregationParams().get(COLUMN_KEY);
  }

  /**
   * Creates an {@link AggregationFunctionColumnPair} from the {@link AggregationInfo}.
   */
  public static AggregationFunctionColumnPair getFunctionColumnPair(AggregationInfo aggregationInfo) {
    AggregationFunctionType functionType =
        AggregationFunctionType.getAggregationFunctionType(aggregationInfo.getAggregationType());
    return new AggregationFunctionColumnPair(functionType, getColumn(aggregationInfo));
  }

  public static boolean isDistinct(AggregationFunctionContext[] functionContexts) {
    return functionContexts.length == 1
        && functionContexts[0].getAggregationFunction().getType() == AggregationFunctionType.DISTINCT;
  }

  /**
   * Creates an {@link AggregationFunctionContext} from the {@link AggregationInfo}.
   */
  public static AggregationFunctionContext getAggregationFunctionContext(AggregationInfo aggregationInfo) {
    return getAggregationFunctionContext(aggregationInfo, null);
  }

  public static AggregationFunctionContext getAggregationFunctionContext(AggregationInfo aggregationInfo,
      @Nullable BrokerRequest brokerRequest) {
    String column = getColumn(aggregationInfo);
    AggregationFunction aggregationFunction =
        AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    return new AggregationFunctionContext(aggregationFunction, column);
  }

  public static AggregationFunctionContext[] getAggregationFunctionContexts(BrokerRequest brokerRequest,
      @Nullable SegmentMetadata segmentMetadata) {
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
      Double doubleValue = (Double) value;

      // String.format is very expensive, so avoid it for whole numbers that can fit in Long.
      // We simply append ".00000" to long, in order to keep the existing behavior.
      if (doubleValue <= Long.MAX_VALUE && DoubleMath.isMathematicalInteger(doubleValue)) {
        return doubleValue.longValue() + ".00000";
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
}
