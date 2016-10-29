package com.linkedin.thirdeye.client.diffsummary;

public class CostFunction {
  public static double err(double v1, double v2, double parentRatio) {
    double expectedValue = parentRatio * v1;
    return (v2 - expectedValue) * Math.log(v2 / expectedValue);
  }

  /**
   * Auto fill in v1 and v2 using parentRatio when one of them is zero.
   * If v1 and v2 both are zero or parentRatio is not finite, this function returns 0.
   */
  public static double err4EmptyValues(double v1, double v2, double parentRatio) {
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
}
