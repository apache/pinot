package com.linkedin.thirdeye.client.diffsummary.costfunction;

public interface CostFunction {
  double getCost(double baselineValue, double currentValue, double parentRatio, double globalBaselineValue,
      double globalCurrentValue);
}
