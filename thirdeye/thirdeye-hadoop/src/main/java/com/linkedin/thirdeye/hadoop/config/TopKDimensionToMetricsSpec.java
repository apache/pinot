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
package com.linkedin.thirdeye.hadoop.config;

import java.util.Map;

/**
 * This class manages config for dimension with topk
 * config defined on multiple metrics
 * @param dimensionName - The dimension of this topk config
 * @param topk - map of metric name to k value
 */
public class TopKDimensionToMetricsSpec {

  String dimensionName;
  Map<String, Integer> topk;

  public TopKDimensionToMetricsSpec() {

  }

  public TopKDimensionToMetricsSpec(String dimensionName, Map<String, Integer> topk) {
    this.dimensionName = dimensionName;
    this.topk = topk;
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public void setDimensionName(String dimensionName) {
    this.dimensionName = dimensionName;
  }

  public Map<String, Integer> getTopk() {
    return topk;
  }

  public void setTopk(Map<String, Integer> topk) {
    this.topk = topk;
  }

  public String toString() {
    return "{ dimensionName : " + dimensionName + ", topk : " + topk + " }";
  }

}
