package com.linkedin.thirdeye.detector.function;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.detector.db.entity.AnomalyResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;

/**
 * Analyses given input based on Normal Distribution Function.
 * <p/>
 * Returns outliers => buckets having value outside [mean (+-) 2 * st. deviation] as AnomalyResults.
 * refer to Normal Distribution https://en.wikipedia.org/wiki/Normal_distribution
 */
public class OutlierAnalysisFunction extends BaseAnomalyFunction {
  private static final Joiner CSV = Joiner.on(",");

  static final String DEFAULT_MESSAGE_TEMPLATE = "mean %s, standard deviation %s, value %s, deviation from mean %s";
  static final String MEAN_STD_DISTANCE_FACTOR_PROP = "meanStdDistanceFactor";
  /**
   * 1, 2, 3 covers 68, 95, 99.7 percent of the distribution mass respectively
   */
  final int DEFAULT_MEAN_STD_DISTANCE_FACTOR = 3;

  public static String[] getPropertyKeys() {
    return new String[] { MEAN_STD_DISTANCE_FACTOR_PROP };
  }

  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<AnomalyResult> knownAnomalies)
      throws Exception {

    List<AnomalyResult> outliers = new ArrayList<>();

    // Metric
    String metric = getSpec().getMetric();

    // Compute the bucket size, so we can iterate in those steps
    long bucketMillis =
        TimeUnit.MILLISECONDS.convert(getSpec().getBucketSize(), getSpec().getBucketUnit());

    List<Double> values = new ArrayList<>();
    double totalSum = 0;
    for (Long timeBucket : timeSeries.getTimeWindowSet()) {
      Double value = timeSeries.get(timeBucket, metric).doubleValue();
      values.add(value);
      totalSum += value;
    }

    long numBuckets = (windowEnd.getMillis() - windowStart.getMillis()) / bucketMillis;

    // weight of this time series
    double averageValue = totalSum / numBuckets;
    int meanStdDistFactor = DEFAULT_MEAN_STD_DISTANCE_FACTOR;

    if (getProperties().containsKey(MEAN_STD_DISTANCE_FACTOR_PROP)) {
      meanStdDistFactor =
          Integer.valueOf(getProperties().getProperty(MEAN_STD_DISTANCE_FACTOR_PROP));
    }

    StatisticalResult statParam = new StatisticalResult(meanStdDistFactor, values);

    for (Long timeBucket : timeSeries.getTimeWindowSet()) {
      Double value = timeSeries.get(timeBucket, metric).doubleValue();
      if (isOutlier(statParam, value)) {
        AnomalyResult outlierResult = new AnomalyResult();
        outlierResult.setCollection(getSpec().getCollection());
        outlierResult.setMetric(metric);
        outlierResult.setDimensions(CSV.join(dimensionKey.getDimensionValues()));
        outlierResult.setFunctionId(getSpec().getId());
        outlierResult.setProperties(getSpec().getProperties());
        outlierResult.setStartTimeUtc(timeBucket);
        outlierResult.setEndTimeUtc(timeBucket + bucketMillis); // point-in-time
        Double deviation = statParam.getMean() - value;
        outlierResult.setScore(Math.abs(deviation)); // higher change, higher the score
        outlierResult.setWeight(averageValue);
        String message = String
            .format(DEFAULT_MESSAGE_TEMPLATE, statParam.getMean(), statParam.getStDeviation(),
                value, deviation);
        outlierResult.setMessage(message);
        outlierResult.setFilters(getSpec().getFilters());
        outliers.add(outlierResult);
      }
    }
    return mergeResults(outliers, DEFAULT_MERGE_TIME_DELTA_MILLIS);
  }

  private static class StatisticalResult {
    private Double mean;
    private Double stDeviation;
    private final int meanStdDistFactor;

    StatisticalResult(int meanStdDistFactor, List<Double> values) {
      this.meanStdDistFactor = meanStdDistFactor;
      analyzeData(values);
    }

    Double getMean() {
      return mean;
    }

    Double getStDeviation() {
      return stDeviation;
    }

    Double getLowerBound() {
      return mean - meanStdDistFactor * stDeviation;
    }

    Double getUpperBound() {
      return mean + meanStdDistFactor * stDeviation;
    }

    private void analyzeData(List<Double> values) {
      Double sum = 0.0;
      Double variance = 0.0;

      for (Double val : values) {
        sum += val;
      }
      mean = sum / values.size();
      for (Double val : values) {
        double diff = mean - val;
        variance += diff * diff;
      }
      stDeviation = Math.sqrt(variance / values.size());
    }
  }

  private boolean isOutlier(StatisticalResult statParam, Double value) {
    if (statParam.getStDeviation() > 0) {
      if(value < statParam.getLowerBound() || value > statParam.getUpperBound()) {
        return true;
      }
    }
    return false;
  }
}
