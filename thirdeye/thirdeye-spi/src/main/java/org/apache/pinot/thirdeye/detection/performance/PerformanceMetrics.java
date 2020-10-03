package org.apache.pinot.thirdeye.detection.performance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


/**
 * The performance metrics of a detection based on the given anomalies
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PerformanceMetrics {

  @JsonProperty
  private PerformanceMetric totalAnomalies;

  @JsonProperty
  private PerformanceMetric responseRate;

  @JsonProperty
  private PerformanceMetric precision;

  @JsonProperty
  private PerformanceMetric recall;

  public PerformanceMetric getTotalAnomalies() { return totalAnomalies; }

  public PerformanceMetric getResponseRate() { return responseRate; }

  public PerformanceMetric getPrecision() { return precision; }

  public PerformanceMetric getRecall() { return recall; }

  /**
   * Builder for performance metrics
   */
  public static class Builder {
    private long respondedAnomalies, truePos, falsePos, falseNeg, numAnomalies = 0L;
    private boolean includeTotalAnomalies, includeResponseRate, includePrecision, includeRecall = false;
    private final String NOT_CLASSIFIED = "NONE";
    private final String TRUE_POSITIVE = "TRUE_POSITIVE";
    private final String FALSE_POSITIVE = "FALSE_POSITIVE";
    private final String FALSE_NEGATIVE = "FALSE_NEGATIVE";

    private PerformanceMetric buildTotalAnomalies() {
      PerformanceMetric totalAnomalies = new PerformanceMetric();
      totalAnomalies.setValue((double)this.numAnomalies);
      totalAnomalies.setType(PerformanceMetricType.COUNT);
      return totalAnomalies;
    }

    private PerformanceMetric buildResponseRate() {
      PerformanceMetric responseRate = new PerformanceMetric();
      double rate = (double)this.respondedAnomalies / this.numAnomalies * 100;
      responseRate.setValue(rate);
      responseRate.setType(PerformanceMetricType.PERCENT);
      return responseRate;
    }

    private PerformanceMetric buildPrecision() {
      PerformanceMetric precision = new PerformanceMetric();
      double prec = (double)this.truePos / (this.truePos + this.falsePos) * 100;
      precision.setValue(prec);
      precision.setType(PerformanceMetricType.PERCENT);
      return precision;
    }

    private PerformanceMetric buildRecall() {
      PerformanceMetric recall = new PerformanceMetric();
      double rec = (double)this.truePos / (this.truePos + this.falseNeg) * 100;
      recall.setValue(rec);
      recall.setType(PerformanceMetricType.PERCENT);
      return recall;
    }

    public Builder (List<String> statusClassifications) {
      statusClassifications.stream()
          .forEach(classification -> {
            this.numAnomalies++;
            if (!NOT_CLASSIFIED.equals(classification)) {
              this.respondedAnomalies++;
              switch (classification) {
                case TRUE_POSITIVE:
                  this.truePos++;
                  break;
                case FALSE_NEGATIVE:
                  this.falseNeg++;
                  break;
                case FALSE_POSITIVE:
                  this.falsePos++;
                  break;
                default:
                  break;
              }
            }
          });
    }

    public Builder addTotalAnomalies() {
      this.includeTotalAnomalies = true;
      return this;
    }

    public Builder addResponseRate() {
      this.includeResponseRate = true;
      return this;
    }

    public Builder addPrecision() {
      this.includePrecision = true;
      return this;
    }

    public Builder addRecall() {
      this.includeRecall = true;
      return this;
    }

    public PerformanceMetrics build() {
      PerformanceMetrics pm = new PerformanceMetrics();
      if (this.includeTotalAnomalies) {
        pm.totalAnomalies = buildTotalAnomalies();
      }
      if (this.includeResponseRate) {
        pm.responseRate = buildResponseRate();
      }
      if (this.includePrecision) {
        pm.precision = buildPrecision();
      }
      if (this.includeRecall) {
        pm.recall = buildRecall();
      }
      return pm;
    }
  }
}
