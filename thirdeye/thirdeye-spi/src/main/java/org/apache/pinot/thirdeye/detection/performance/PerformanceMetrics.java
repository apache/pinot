package org.apache.pinot.thirdeye.detection.performance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;


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

    private PerformanceMetric buildTotalAnomalies() {
      PerformanceMetric totalAnomalies = new PerformanceMetric();
      totalAnomalies.setValue((double) this.numAnomalies);
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

    /***
     * Builds the performance given a list of anomalies. When calculating entity anomalies, it counts the total number of anomalies
     * only on the parent level but includes children anomalies when calculating the performance
     * @param anomalies A list of anomalies
     */
    public Builder (List<MergedAnomalyResultDTO> anomalies) {
      anomalies.stream()
          .forEach(anomaly -> {
            if (!anomaly.isChild()) {
              this.numAnomalies++;
            }

            if (anomaly.getAnomalyResultSource() != null) {
              if (AnomalyResultSource.USER_LABELED_ANOMALY.equals(anomaly.getAnomalyResultSource())) {
                if (anomaly.getFeedback() == null || anomaly.getFeedback().getFeedbackType().isAnomaly()) {
                  // NOTE: includes user-created anomaly without feedback as false negative by default
                  this.falseNeg++;
                  this.respondedAnomalies++;
                  return;
                }

                if (anomaly.getFeedback().getFeedbackType().isNotAnomaly()) {
                  this.respondedAnomalies++;
                  return;
                }
              }
            }

            if (anomaly.getFeedback() == null) {
              return;
            }

            switch (anomaly.getFeedback().getFeedbackType()) {
              case ANOMALY:
              case ANOMALY_EXPECTED:
              case ANOMALY_NEW_TREND:
                this.truePos++;
                this.respondedAnomalies++;
                break;
              case NOT_ANOMALY:
                this.falsePos++;
                this.respondedAnomalies++;
                break;
              default:
                break;
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
