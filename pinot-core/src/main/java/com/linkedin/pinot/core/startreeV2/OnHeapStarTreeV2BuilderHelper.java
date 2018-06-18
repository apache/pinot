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

package com.linkedin.pinot.core.startreeV2;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import com.google.common.collect.ListMultimap;
import com.linkedin.pinot.common.data.MetricFieldSpec;


public class OnHeapStarTreeV2BuilderHelper {

  public OnHeapStarTreeV2BuilderHelper() {
  }

  /**
   * Extract metric field spec from the met2aggFuncPairs List.
   */
  public static List<MetricFieldSpec> getMetricFieldSpec(ListMultimap<MetricFieldSpec, String>met2aggfuncPairs) {
    List<MetricFieldSpec> metricFieldSpecList = new ArrayList<>();
    for (MetricFieldSpec metric: met2aggfuncPairs.keySet()) {
      metricFieldSpecList.add(metric);
    }
    return metricFieldSpecList;
  }

  /**
   * compute defualt split order.
   */
  public static List<Integer> computeDefaultSplitOrder(int dimensionsCount) {
    List<Integer> defaultSplitOrder = new ArrayList<>();
    for (int i = 0; i < dimensionsCount; i++) {
      defaultSplitOrder.add(i);
    }
    return defaultSplitOrder;
  }

  /**
   * Convert string based Dimension split order to index based split order.
   */
  public static List<Integer> getEnumeratedDimensionSplitOrder(List<String>dimensionsSplitOrder, List<String>dimensionsName) {

    List<Integer> enumeratedDimensionsSplitOrder = new ArrayList<>(dimensionsSplitOrder.size());
    for (String dimensionName : dimensionsSplitOrder) {
      enumeratedDimensionsSplitOrder.add(dimensionsName.indexOf(dimensionName));
    }
    return enumeratedDimensionsSplitOrder;
  }
}
