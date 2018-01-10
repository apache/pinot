package com.linkedin.thirdeye.client.diffsummary.costfunction;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Map;

public class BalancedCostFunction implements CostFunction {
  public static final String CONTRIBUTION_PERCENTAGE_THRESHOLD_PARAM = "pctThreshold";
  private double contributionPercentageThreshold = 3d;

  public BalancedCostFunction() {
  }

  public BalancedCostFunction(Map<String, String> params) {
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
  public double getCost(double baselineValue, double currentValue, double parentRatio, double globalBaselineValue,
      double globalCurrentValue) {
    double globalSum = globalBaselineValue + globalCurrentValue;
    double percentageContribution = (((baselineValue + currentValue) / globalSum) * 100);
    if (Double.compare(percentageContribution, contributionPercentageThreshold) < 0) {
      return 0d;
    } else {
      return err4EmptyValues(baselineValue, currentValue, parentRatio) * Math.log(percentageContribution);
    }
  }

  private static double err(double baselineValue, double currentValue, double parentRatio) {
    double expectedBaselineValue = parentRatio * baselineValue;
    return (currentValue - expectedBaselineValue) * Math.log(currentValue / expectedBaselineValue);
  }

  /**
   * Auto fill in baselineValue and currentValue using parentRatio when one of them is zero.
   * If baselineValue and currentValue both are zero or parentRatio is not finite, this function returns 0.
   */
  private static double err4EmptyValues(double baselineValue, double currentValue, double parentRatio) {
    if (Double.compare(0., baselineValue) != 0 && Double.compare(0., currentValue) != 0) {
      return err(baselineValue, currentValue, parentRatio);
    } else if (Double.compare(baselineValue, 0d) == 0 || Double.compare(currentValue, 0d) == 0) {
      double filledInRatio = Math.abs(baselineValue - currentValue);
      if (Double.compare(filledInRatio, Math.E) < 0) {
        filledInRatio = 1d;
      } else {
        filledInRatio = Math.log(filledInRatio);
      }
      if (Double.compare(0., baselineValue) == 0) {
        return err(currentValue / Math.max(filledInRatio, parentRatio + (1/filledInRatio)), currentValue, parentRatio);
      } else {
        filledInRatio = 1d / filledInRatio; // because Double.compare(baselineValue, currentValue) > 0
        return err(baselineValue, baselineValue * Math.min(1/filledInRatio, parentRatio + filledInRatio), parentRatio);
      }
    } else { // baselineValue and currentValue are zeros. Set cost to zero so the node will be naturally aggregated to its parent.
      return 0.;
    }
  }
}
