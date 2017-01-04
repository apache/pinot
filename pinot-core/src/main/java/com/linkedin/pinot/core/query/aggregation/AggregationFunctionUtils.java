/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.query.aggregation;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.operator.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.plan.AggregationFunctionInitializer;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * The <code>AggregationFunctionUtils</code> class provides utility methods for aggregation function.
 */
public class AggregationFunctionUtils {
  private AggregationFunctionUtils() {
  }

  @Nonnull
  public static AggregationFunctionContext[] getAggregationFunctionContexts(
      @Nonnull List<AggregationInfo> aggregationInfos, @Nullable SegmentMetadata segmentMetadata) {
    int numAggregationFunctions = aggregationInfos.size();
    AggregationFunctionContext[] aggregationFunctionContexts = new AggregationFunctionContext[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationInfo aggregationInfo = aggregationInfos.get(i);
      aggregationFunctionContexts[i] = AggregationFunctionContext.instantiate(aggregationInfo);
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
}
