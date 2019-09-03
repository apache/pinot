/*
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

package org.apache.pinot.thirdeye.cube.cost;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Map;

public class ContributionToOverallChangeCostFunction implements CostFunction {
  public static final String CONTRIBUTION_PERCENTAGE_THRESHOLD_PARAM = "pctThreshold";
  private double contributionPercentageThreshold = 3d;

  public ContributionToOverallChangeCostFunction() {
  }

  public ContributionToOverallChangeCostFunction(Map<String, String> params) {
    if (params.containsKey(CONTRIBUTION_PERCENTAGE_THRESHOLD_PARAM)) {
      String pctThresholdString = params.get(CONTRIBUTION_PERCENTAGE_THRESHOLD_PARAM);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(pctThresholdString));
      this.contributionPercentageThreshold = Double.parseDouble(pctThresholdString);
    }
  }

  public double getContributionPercentageThreshold() {
    return contributionPercentageThreshold;
  }

  public void setContributionPercentageThreshold(double contributionPercentageThreshold) {
    this.contributionPercentageThreshold = contributionPercentageThreshold;
  }

  @Override
  public double computeCost(double parentChangeRatio, double baselineValue, double currentValue, double baselineSize,
      double currentSize, double topBaselineValue, double topCurrentValue, double topBaselineSize,
      double topCurrentSize) {

    double contributionToOverallChange = (currentValue - baselineValue) / (topCurrentValue - topBaselineValue);
    double percentageContribution = (((baselineValue) / topBaselineValue) * 100);
    if (Double.compare(percentageContribution, contributionPercentageThreshold) < 0) {
      return 0d;
    } else {
      return contributionToOverallChange / percentageContribution;
    }
  }
}
