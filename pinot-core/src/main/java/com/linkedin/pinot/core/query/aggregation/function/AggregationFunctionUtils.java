/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import com.google.common.math.DoubleMath;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.plan.AggregationFunctionInitializer;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * The <code>AggregationFunctionUtils</code> class provides utility methods for aggregation function.
 */
public class AggregationFunctionUtils {
  private AggregationFunctionUtils() {
  }

  public static final String COLUMN_KEY = "column";

  /**
   * Extracts the aggregation column (could be column name or UDF expression) from the {@link AggregationInfo}.
   */
  @Nonnull
  public static String getColumn(@Nonnull AggregationInfo aggregationInfo) {
    return aggregationInfo.getAggregationParams().get(COLUMN_KEY);
  }

  /**
   * Creates an {@link AggregationFunctionColumnPair} from the {@link AggregationInfo}.
   */
  @Nonnull
  public static AggregationFunctionColumnPair getFunctionColumnPair(@Nonnull AggregationInfo aggregationInfo) {
    AggregationFunctionType functionType =
        AggregationFunctionType.getAggregationFunctionType(aggregationInfo.getAggregationType());
    return new AggregationFunctionColumnPair(functionType, getColumn(aggregationInfo));
  }

  /**
   * Creates an {@link AggregationFunctionContext} from the {@link AggregationInfo}.
   */
  @Nonnull
  public static AggregationFunctionContext getAggregationFunctionContext(@Nonnull AggregationInfo aggregationInfo) {
    String functionName = aggregationInfo.getAggregationType();
    AggregationFunction aggregationFunction = AggregationFunctionFactory.getAggregationFunction(functionName);
    return new AggregationFunctionContext(aggregationFunction, AggregationFunctionUtils.getColumn(aggregationInfo));
  }

  @Nonnull
  public static AggregationFunctionContext[] getAggregationFunctionContexts(
      @Nonnull List<AggregationInfo> aggregationInfos, @Nullable SegmentMetadata segmentMetadata) {
    int numAggregationFunctions = aggregationInfos.size();
    AggregationFunctionContext[] aggregationFunctionContexts = new AggregationFunctionContext[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationInfo aggregationInfo = aggregationInfos.get(i);
      aggregationFunctionContexts[i] = getAggregationFunctionContext(aggregationInfo);
    }
    if (segmentMetadata != null) {
      AggregationFunctionInitializer aggregationFunctionInitializer =
          new AggregationFunctionInitializer(segmentMetadata);
      for (AggregationFunctionContext aggregationFunctionContext : aggregationFunctionContexts) {
        aggregationFunctionContext.getAggregationFunction().accept(aggregationFunctionInitializer);
      }
    }
    return aggregationFunctionContexts;
  }

  @Nonnull
  public static AggregationFunction[] getAggregationFunctions(@Nonnull List<AggregationInfo> aggregationInfos) {
    int numAggregationFunctions = aggregationInfos.size();
    AggregationFunction[] aggregationFunctions = new AggregationFunction[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      aggregationFunctions[i] =
          AggregationFunctionFactory.getAggregationFunction(aggregationInfos.get(i).getAggregationType());
    }
    return aggregationFunctions;
  }

  @Nonnull
  public static boolean[] getAggregationFunctionsSelectStatus(@Nonnull List<AggregationInfo> aggregationInfos) {
    int numAggregationFunctions = aggregationInfos.size();
    boolean[] aggregationFunctionsStatus = new boolean[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      aggregationFunctionsStatus[i] = aggregationInfos.get(i).isIsInSelectList();
    }
    return aggregationFunctionsStatus;
  }

  @Nonnull
  public static String formatValue(@Nonnull Object value) {
    if (value instanceof Double) {
      Double doubleValue = (Double) value;

      // String.format is very expensive, so avoid it for whole numbers that can fit in Long.
      // We simply append ".00000" to long, in order to keep the existing behavior.
      if (doubleValue <= Long.MAX_VALUE && DoubleMath.isMathematicalInteger(doubleValue)) {
        return Long.toString(doubleValue.longValue()) + ".00000";
      } else {
        return String.format(Locale.US, "%1.5f", doubleValue);
      }
    } else {
      return value.toString();
    }
  }

  @Nonnull
  public static Serializable getSerializableValue(@Nonnull Object value) {
    if (value instanceof Number) {
      return (Number) value;
    } else {
      return value.toString();
    }
  }
}
