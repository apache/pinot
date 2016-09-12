package com.linkedin.thirdeye.anomaly.alert;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class AnomalyFiltrationHelper {

  public static FiltrationRule getFiltrationRule() {
    // TODO : create filtration rule factory based on type and json value passed from db
    return new AlphaBetaFiltrationRule(1, 1, 0.8);
  }

  public enum RuleType { AlphaBeta }

  interface FiltrationRule {
    boolean isQualified(MergedAnomalyResultDTO anomaly);
  }

  public static class AlphaBetaFiltrationRule implements FiltrationRule {
    final Double alpha, beta, threshold;

    AlphaBetaFiltrationRule(double alpha, double beta, double threshold) {
      this.alpha = alpha;
      this.beta = beta;
      this.threshold = threshold;
    }

    @Override
    public boolean isQualified(MergedAnomalyResultDTO anomaly) {
      Double lengthInHour =
          (anomaly.getEndTime().doubleValue() - anomaly.getStartTime().doubleValue()) / 36_00_000;
      // In thirdeye weight is severity
      Double qualificationScore =
          Math.pow(lengthInHour, alpha) * Math.pow(Math.abs(anomaly.getWeight()), beta);
      return (qualificationScore > threshold);
    }
  }
}

