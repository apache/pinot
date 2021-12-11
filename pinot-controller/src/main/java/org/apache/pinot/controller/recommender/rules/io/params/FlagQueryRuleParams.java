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

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.FlagQueryRuleParams.DEFAULT_THRESHOLD_MAX_LIMIT_SIZE;


/**
 * Thresholds and parameters used in FlagQueryRule
 */
public class FlagQueryRuleParams {
  // Maximum x in predicate "LIMIT x" beyond that the query is too expensive
  public Long _thresholdMaxLimitSize = DEFAULT_THRESHOLD_MAX_LIMIT_SIZE;

  public Long getThresholdMaxLimitSize() {
    return _thresholdMaxLimitSize;
  }

  @JsonSetter(value = "THRESHOLD_MAX_LIMIT_SIZE", nulls = Nulls.SKIP)
  public void setThresholdMaxLimitSize(Long thresholdMaxLimitSize) {
    _thresholdMaxLimitSize = thresholdMaxLimitSize;
  }
}
