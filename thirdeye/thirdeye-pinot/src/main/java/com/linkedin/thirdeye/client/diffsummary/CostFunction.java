package com.linkedin.thirdeye.client.diffsummary;

public class CostFunction {
  private static double err(double baselineValue, double currentValue, double parentRatio) {
    double expectedBaselineValue = parentRatio * baselineValue;
    return (currentValue - expectedBaselineValue) * Math.log(currentValue / expectedBaselineValue);
  }

  /**
   * Auto fill in v1 and v2 using parentRatio when one of them is zero.
   * If v1 and v2 both are zero or parentRatio is not finite, this function returns 0.
   */
  private static double err4EmptyValues(double v1, double v2, double parentRatio) {
    if (Double.compare(0., v1) != 0 && Double.compare(0., v2) != 0) {
      return CostFunction.err(v1, v2, parentRatio);
    } else if (Double.compare(v1, 0d) == 0 || Double.compare(v2, 0d) == 0) {
      double filledInRatio = Math.abs(v1 - v2);
      if (Double.compare(filledInRatio, Math.E) < 0) {
        filledInRatio = 1d;
      } else {
        filledInRatio = Math.log(filledInRatio);
      }
      if (Double.compare(0., v1) == 0) {
        return CostFunction.err(v2 / Math.max(filledInRatio, parentRatio + (1/filledInRatio)), v2, parentRatio);
      } else {
        filledInRatio = 1d / filledInRatio; // because Double.compare(v1, v2) > 0
        return CostFunction.err(v1, v1 * Math.min(1/filledInRatio, parentRatio + filledInRatio), parentRatio);
      }
    } else { // v1 and v2 are zeros. Set cost to zero so the node will be naturally aggregated to its parent.
      return 0.;
    }
  }

  public double errWithPercentageRemoval(double baselineValue, double currentValue, double parentRatio,
      double threshold, double globalBaselineValue, double globalCurrentValue) {
    double globalSum = globalBaselineValue + globalCurrentValue;
    if (false) {
      double contributionToOverallChange = (currentValue - baselineValue) / (globalCurrentValue - globalBaselineValue);
      double percentageContribution = (((baselineValue ) / globalBaselineValue) * 100);
      if (Double.compare(percentageContribution, threshold) < 0) {
        return 0d;
      } else {
        return contributionToOverallChange / percentageContribution;
      }
    } else {
      double percentageContribution = (((baselineValue + currentValue) / globalSum) * 100);
      if (Double.compare(percentageContribution, threshold) < 0) {
        return 0d;
      } else {
        return err4EmptyValues(baselineValue, currentValue, parentRatio) * Math.log(percentageContribution);
      }
    }
  }
}
