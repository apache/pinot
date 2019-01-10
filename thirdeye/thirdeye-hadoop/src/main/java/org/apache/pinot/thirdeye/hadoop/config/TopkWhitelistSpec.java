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
package com.linkedin.thirdeye.hadoop.config;

import java.util.List;
import java.util.Map;

/**
 * Config class to define topk and whitelist
 *
 * @param threshold - dimension values which do not satisfy metric thresholds will be ignored.
 * The metric total contributed by a dimension will be compared with the metric total across all the records.
 *
 * @param topKDimensionToMetricsSpec - list of dimension and a map of metric to topk value for that dimension
 * Only top k values for the dimension will be picked, based on metric
 *
 * @param whitelist - values to whitelist for given dimension (dimension:whitelist values)
 *
 * @param non whitelist value - value to be used for a dimension value, which is not in whitelist
 */
public class TopkWhitelistSpec {

  Map<String, Double> threshold;
  List<TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec;
  Map<String, List<String>> whitelist;
  Map<String, String> nonWhitelistValue;

  public TopkWhitelistSpec() {

  }

  public Map<String, Double> getThreshold() {
    return threshold;
  }

  public void setThreshold(Map<String, Double> threshold) {
    this.threshold = threshold;
  }

  public List<TopKDimensionToMetricsSpec> getTopKDimensionToMetricsSpec() {
    return topKDimensionToMetricsSpec;
  }

  public void setTopKDimensionToMetricsSpec(List<TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec) {
    this.topKDimensionToMetricsSpec = topKDimensionToMetricsSpec;
  }

  public Map<String, List<String>> getWhitelist() {
    return whitelist;
  }

  public void setWhitelist(Map<String, List<String>> whitelist) {
    this.whitelist = whitelist;
  }

  public Map<String, String> getNonWhitelistValue() {
    return nonWhitelistValue;
  }

  public void setNonWhitelistValue(Map<String, String> nonWhitelistValue) {
    this.nonWhitelistValue = nonWhitelistValue;
  }

}
