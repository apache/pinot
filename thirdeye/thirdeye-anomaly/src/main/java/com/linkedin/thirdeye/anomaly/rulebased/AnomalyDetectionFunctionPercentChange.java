package com.linkedin.thirdeye.anomaly.rulebased;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.ResultProperties;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;
import com.linkedin.thirdeye.anomaly.exception.FunctionDidNotEvaluateException;
import com.linkedin.thirdeye.anomaly.exception.IllegalFunctionException;
import com.linkedin.thirdeye.anomaly.util.DimensionKeyMatchTable;
import com.linkedin.thirdeye.anomaly.util.TimeGranularityUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

/**
 *
 */
public class AnomalyDetectionFunctionPercentChange extends AnomalyDetectionFunctionAbstractBase {

  private TimeGranularity baselineTimePeriod;
  private String metricName;
  private double percentDelta;

  private DimensionKeyMatchTable<Double> deltaTable;

  public AnomalyDetectionFunctionPercentChange(TimeGranularity baselineTimePeriod,
      TimeGranularity aggregateTimeGranularity, String metricName, double percentDelta) {
    super(aggregateTimeGranularity);
    this.baselineTimePeriod = baselineTimePeriod;
    this.metricName = metricName;
    this.percentDelta = percentDelta;
  }

  @Override
  public void init(StarTreeConfig starTreeConfig, FunctionProperties functionConfig) throws IllegalFunctionException {
    super.init(starTreeConfig, functionConfig);
  }

  @Override
  public TimeGranularity getTrainingWindowTimeGranularity() {
    return baselineTimePeriod;
  }

  @Override
  public Set<String> getMetrics() {
    Set<String> metrics = new HashSet<>();
    metrics.add(metricName);
    return metrics;
  }

  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries series, TimeRange timeInterval,
      List<AnomalyResult> anomalyHistory) {
    if (series.getSchema().getNames().contains(metricName) == false) {
      throw new FunctionDidNotEvaluateException("'" + metricName + "' does not exist in the MetricTimeSeries");
    }

    List<AnomalyResult> results = new ArrayList<AnomalyResult>();

    // get the effective threshold from the delta table
    double threshold = getEffectiveThreshold(dimensionKey);

    int pointsEvaluated = 0;
    for (Long timeWindow : series.getTimeWindowSet()) {
      if (timeInterval.contains(timeWindow) == false) {
        continue;
      }

      long oldTime = timeWindow - TimeGranularityUtils.toMillis(baselineTimePeriod);

      // compare value against the baseline
      if (series.getTimeWindowSet().contains(oldTime)) {
        double oldValue = series.get(oldTime, metricName).doubleValue();
        double newValue = series.get(timeWindow, metricName).doubleValue();
        double percentChange;
        if (oldValue > 0) {
          percentChange = 100.0 * (newValue - oldValue) / oldValue;
        } else {
          percentChange = 0.0;
        }
        double absoluteChange = newValue - oldValue;

        ResultProperties properties = new ResultProperties();
        properties.setProperty("oldValue", "" + oldValue);
        properties.setProperty("newValue", "" + newValue);
        properties.setProperty("usingThreshold", "" + threshold);
        properties.setProperty("absoluteChange", "" + absoluteChange);

        boolean isAnomaly;
        // percent delta determines the direction of the rule
        if (percentDelta < 0) {
            isAnomaly = percentChange <= threshold;
        } else {
            isAnomaly = percentChange >= threshold;
        }

        results.add(new AnomalyResult(isAnomaly, timeWindow, percentChange, absoluteChange, properties));
        pointsEvaluated++;
      }
    }

    if (pointsEvaluated == 0) {
      throw new FunctionDidNotEvaluateException("not enough data present in the MetricTimeSeries");
    }

    return results;
  }

  public String toString() {
    return String.format("%s change in %s exceeds %.2f%%", baselineTimePeriod.toString(), metricName, percentDelta);
  }

  public AnomalyDetectionFunction setDeltaTable(DimensionKeyMatchTable<Double> deltaTable) {
    this.deltaTable = deltaTable;
    return this;
  }

  /**
   * @return
   *  The threshold to use based on the best match table, or default if no match is found or no table is specified.
   */
  private double getEffectiveThreshold(DimensionKey dimensionKey) {
    if (deltaTable != null) {
      Double threshold = deltaTable.get(dimensionKey);
      if (threshold != null) {
        return threshold;
      }
    }
    return percentDelta;
  }


}
