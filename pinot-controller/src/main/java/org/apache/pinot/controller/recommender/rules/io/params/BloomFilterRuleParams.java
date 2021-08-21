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
package org.apache.pinot.controller.recommender.rules.io.params;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;


/**
 * Thresholds and parameters used in BloomFilterRule
 */
public class BloomFilterRuleParams {
  // The minimum percentage of queries using a EQ predicate on a given dimension, which we want to optimize with
  // BloomFilter
  public Double _thresholdMinPercentEqBloomfilter =
      RecommenderConstants.BloomFilterRule.DEFAULT_THRESHOLD_MIN_PERCENT_EQ_BLOOMFILTER;

  //Beyond this cardinality the bloom filter grows larger than 1MB, and we currently limit the size to 1MB
  public Long _thresholdMaxCardinalityBloomfilter =
      RecommenderConstants.BloomFilterRule.DEFAULT_THRESHOLD_MAX_CARDINALITY_BLOOMFILTER;

  public Double getThresholdMinPercentEqBloomfilter() {
    return _thresholdMinPercentEqBloomfilter;
  }

  @JsonSetter(value = "THRESHOLD_MIN_PERCENT_EQ_BLOOMFILTER", nulls = Nulls.SKIP)
  public void setThresholdMinPercentEqBloomfilter(Double thresholdMinPercentEqBloomfilter) {
    _thresholdMinPercentEqBloomfilter = thresholdMinPercentEqBloomfilter;
  }

  public Long getThresholdMaxCardinalityBloomfilter() {
    return _thresholdMaxCardinalityBloomfilter;
  }

  @JsonSetter(value = "THRESHOLD_MAX_CARDINALITY_BLOOMFILTER", nulls = Nulls.SKIP)
  public void setThresholdMaxCardinalityBloomfilter(Long thresholdMaxCardinalityBloomfilter) {
    _thresholdMaxCardinalityBloomfilter = thresholdMaxCardinalityBloomfilter;
  }
}
