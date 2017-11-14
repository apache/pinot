/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.hadoop.topk;

/**
 * Class to manage dimension value and metric values pairs
 * The order of elements is determined based on the metric value -
 * Lesser metric value is treated as greater in ordering,
 * so that it gets removed from the fixed size PriorityQueue first
 */
public class DimensionValueMetricPair implements Comparable<DimensionValueMetricPair>{

  private Object dimensionValue;
  private Number metricValue;

  public DimensionValueMetricPair(Object dimensionValue, Number metricValue) {
    this.dimensionValue = dimensionValue;
    this.metricValue = metricValue;
  }

  public Object getDimensionValue() {
    return dimensionValue;
  }
  public void setDimensionValue(Object dimensionValue) {
    this.dimensionValue = dimensionValue;
  }
  public Number getMetricValue() {
    return metricValue;
  }
  public void setMetricValue(Number metricValue) {
    this.metricValue = metricValue;
  }


  @Override
  public int compareTo(DimensionValueMetricPair other) {
    return other.metricValue.intValue() - this.metricValue.intValue();
  }

  @Override
  public String toString() {
    return "[" + dimensionValue + "=" + metricValue + "]";
  }



}
