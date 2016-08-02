package com.linkedin.thirdeye.client.pinot.summary;

class CostFunction {
  static double err(double v1, double v2, double parentRatio) {
    // TODO: Calculated incrementally
    double expectedValue = parentRatio * v1;
    return (v2 - expectedValue) * Math.log(v2 / expectedValue);
  }

  /**
   * Auto fill in v1 or v2 using parentRatio when v1 or v2 is zero.
   * If v1 and v2 are zero or parentRatio is not finite, this function returns 0.
   */
  static double err4EmptyValues(double v1, double v2, double parentRatio) {
    if (!Double.isFinite(parentRatio)) { return 0.; }
    if (Double.compare(0., v1) != 0 && Double.compare(0., v2) != 0) {
      return CostFunction.err(v1, v2, parentRatio);
    } else {
      if (Double.compare(0., v1) == 0 && Double.compare(0., v2) == 0) {
        return 0.;
      } else {
        if (Double.compare(0., v1) == 0) {
          return CostFunction.err(v2 / parentRatio, v2, parentRatio);
        } else {
          return CostFunction.err(v1, v1 * parentRatio, parentRatio);
        }
      }
    }
  }

  static double err4EmptyValues(double v1, double v2, double parentRatio, double F) {
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
