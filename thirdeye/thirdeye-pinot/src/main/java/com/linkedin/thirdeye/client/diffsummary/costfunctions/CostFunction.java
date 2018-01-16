package com.linkedin.thirdeye.client.diffsummary.costfunctions;

public interface CostFunction {
  double computeCost(double baselineValue, double currentValue, double parentRatio, double globalBaselineValue,
      double globalCurrentValue);
}
