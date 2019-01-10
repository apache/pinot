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
package com.linkedin.thirdeye.hadoop.util;

import java.util.List;

import com.linkedin.thirdeye.hadoop.config.MetricType;

/**
 * Class to aggregate metric values
 */
public class ThirdeyeAggregateMetricUtils {

  /**
   * Aggregates an array of metricValues into an aggregate array
   * @param metricTypes - metric types array
   * @param aggMetricValues - aggregated metric values
   * @param metricValues - metric values to add
   */
  public static void aggregate(List<MetricType> metricTypes, Number[] aggMetricValues, Number[] metricValues) {
    int numMetrics = aggMetricValues.length;
    for (int i = 0; i < numMetrics; i++) {
      MetricType metricType = metricTypes.get(i);
      switch (metricType) {
        case SHORT:
          aggMetricValues[i] = aggMetricValues[i].shortValue() + metricValues[i].shortValue();
          break;
        case INT:
          aggMetricValues[i] = aggMetricValues[i].intValue() + metricValues[i].intValue();
          break;
        case FLOAT:
          aggMetricValues[i] = aggMetricValues[i].floatValue() + metricValues[i].floatValue();
          break;
        case DOUBLE:
          aggMetricValues[i] = aggMetricValues[i].doubleValue() + metricValues[i].doubleValue();
          break;
        case LONG:
        default:
          aggMetricValues[i] = aggMetricValues[i].longValue() + metricValues[i].longValue();
          break;
      }
    }
  }

}
