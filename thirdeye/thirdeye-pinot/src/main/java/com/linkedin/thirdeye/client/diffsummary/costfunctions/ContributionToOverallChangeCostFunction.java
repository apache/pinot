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
