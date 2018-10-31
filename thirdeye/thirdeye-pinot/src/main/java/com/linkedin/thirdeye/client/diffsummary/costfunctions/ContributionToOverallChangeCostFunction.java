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

package com.linkedin.thirdeye.client.diffsummary.costfunctions;

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
  public double computeCost(double baselineValue, double currentValue, double parentRatio, double globalBaselineValue,
      double globalCurrentValue) {

    double contributionToOverallChange = (currentValue - baselineValue) / (globalCurrentValue - globalBaselineValue);
    double percentageContribution = (((baselineValue) / globalBaselineValue) * 100);
    if (Double.compare(percentageContribution, contributionPercentageThreshold) < 0) {
      return 0d;
    } else {
      return contributionToOverallChange / percentageContribution;
    }
  }
}
