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
    return err4EmptyValues(v1, v2, parentRatio, parentRatio);
  }

  public static double err4EmptyValues(double v1, double v2, double parentRatio, double F) {
    if (!Double.isFinite(parentRatio)) { return 0.; }
    if (Double.compare(0., v1) != 0 && Double.compare(0., v2) != 0) {
      return CostFunction.err(v1, v2, parentRatio);
    } else {
      if (Double.compare(0., v1) == 0 && Double.compare(0., v2) == 0) {
        return 0.;
      } else {
        if (Double.compare(0., v1) == 0) {
          return CostFunction.err(v2 / Math.max(F, parentRatio + (1/F)), v2, parentRatio);
        } else {
          return CostFunction.err(v1, v1 * Math.min(1/F, parentRatio), parentRatio);
        }
      }
    }
  }
}
