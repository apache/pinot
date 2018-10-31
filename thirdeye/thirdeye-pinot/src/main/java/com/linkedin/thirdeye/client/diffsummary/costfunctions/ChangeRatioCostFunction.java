package com.linkedin.thirdeye.client.diffsummary.costfunctions;

public class ChangeRatioCostFunction implements CostFunction {
  @Override
  public double computeCost(double baselineValue, double currentValue, double parentRatio, double globalBaselineValue,
      double globalCurrentValue) {
    return fillEmptyValuesAndGetError(baselineValue, currentValue, parentRatio);
  }

  private static double error(double baselineValue, double currentValue, double parentRatio) {
    double expectedBaselineValue = parentRatio * baselineValue;
    return (currentValue - expectedBaselineValue) * Math.log(currentValue / expectedBaselineValue);
  }

  /**
   * Auto fill in baselineValue and currentValue using parentRatio when one of them is zero.
   * If baselineValue and currentValue both are zero or parentRatio is not finite, this function returns 0.
   */
  private static double fillEmptyValuesAndGetError(double baselineValue, double currentValue, double parentRatio) {
    if (Double.compare(0., parentRatio) == 0 || Double.isNaN(parentRatio)) {
      parentRatio = 1d;
    }
    if (Double.compare(0., baselineValue) != 0 && Double.compare(0., currentValue) != 0) {
      return error(baselineValue, currentValue, parentRatio);
    } else if (Double.compare(baselineValue, 0d) == 0 || Double.compare(currentValue, 0d) == 0) {
      double filledInRatio = Math.max(1d, Math.abs(baselineValue - currentValue));
      if (Double.compare(0., baselineValue) == 0) {
        return error(currentValue / Math.max(filledInRatio, parentRatio + (1 / filledInRatio)), currentValue,
            parentRatio);
      } else {
        filledInRatio = 1d / filledInRatio; // because Double.compare(baselineValue, currentValue) > 0
        return error(baselineValue, baselineValue * Math.min(1 / filledInRatio, parentRatio + filledInRatio),
            parentRatio);
      }
    } else { // baselineValue and currentValue are zeros. Set cost to zero so the node will be naturally aggregated to its parent.
      return 0.;
    }
  }
}
