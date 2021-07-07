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
 * Thresholds and parameters used in @RangeIndexRule
 */
public class RangeIndexRuleParams {
  public Double THRESHOLD_MIN_PERCENT_RANGE_INDEX = RecommenderConstants.RangeIndexRule.DEFAULT_THRESHOLD_MIN_PERCENT_RANGE_INDEX;

  public Double getTHRESHOLD_MIN_PERCENT_RANGE_INDEX() {
    return THRESHOLD_MIN_PERCENT_RANGE_INDEX;
  }

  @JsonSetter(value = "THRESHOLD_MIN_PERCENT_RANGE_INDEX", nulls = Nulls.SKIP)
  public void setTHRESHOLD_MIN_PERCENT_RANGE_INDEX(Double THRESHOLD_MIN_PERCENT_RANGE_INDEX) {
    this.THRESHOLD_MIN_PERCENT_RANGE_INDEX = THRESHOLD_MIN_PERCENT_RANGE_INDEX;
  }
}
