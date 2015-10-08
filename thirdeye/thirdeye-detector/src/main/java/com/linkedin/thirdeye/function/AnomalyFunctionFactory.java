package com.linkedin.thirdeye.function;

import com.linkedin.thirdeye.api.AnomalyFunctionSpec;

public class AnomalyFunctionFactory {
  private enum Type {
    KALMAN_FILTER,
    SCAN_STATISTICS,
    USER_RULE
  }

  public static AnomalyFunction fromSpec(AnomalyFunctionSpec functionSpec) throws Exception {
    AnomalyFunction anomalyFunction = null;

    Type type = Type.valueOf(functionSpec.getType().toUpperCase());
    switch (type) {
      case KALMAN_FILTER:
        anomalyFunction = new KalmanFilterAnomalyFunction();
        break;
      case SCAN_STATISTICS:
        anomalyFunction = new ScanStatisticsAnomalyFunction();
        break;
      case USER_RULE:
        anomalyFunction = new UserRuleAnomalyFunction();
        break;
      default:
        throw new IllegalArgumentException("Unsupported type " + type);
    }

    anomalyFunction.init(functionSpec);

    return anomalyFunction;
  }
}
