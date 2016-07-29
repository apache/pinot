package com.linkedin.thirdeye.client.pinot.summary;

class CostFunction {
  static double err(double v1, double v2, double parentRatio) {
    // TODO: Calculated incrementally
    double expectedValue = parentRatio * v1;
    return (v2 - expectedValue) * Math.log(v2 / expectedValue);
  }
}
