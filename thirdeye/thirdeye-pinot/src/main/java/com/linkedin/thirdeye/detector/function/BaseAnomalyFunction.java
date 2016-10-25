package com.linkedin.thirdeye.detector.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseAnomalyFunction implements AnomalyFunction {

  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  private AnomalyFunctionDTO spec;

  @Override
  public void init(AnomalyFunctionDTO spec) throws Exception {
    this.spec = spec;
  }

  @Override
  public AnomalyFunctionDTO getSpec() {
    return spec;
  }

  protected Properties getProperties() throws IOException {
    Properties props = new Properties();
    if (spec.getProperties() != null) {
      String[] tokens = spec.getProperties().split(";");
      for (String token : tokens) {
        props.load(new ByteArrayInputStream(token.getBytes()));
      }
    }
    return props;
  }

  /**
   * Returns unit change from baseline value
   * @param currentValue
   * @param baselineValue
   * @return
   */
  protected double calculateChange(double currentValue, double baselineValue) {
    return (currentValue - baselineValue) / baselineValue;
  }

  /**
   * Useful when multiple time intervals are needed for fetching current vs baseline data
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return
   */
  public List<Pair<Long, Long>> getDataRangeIntervals(Long monitoringWindowStartTime,
      Long monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime, monitoringWindowEndTime));

    long baselineOffsetMillis = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
    startEndTimeIntervals.add(
        new Pair<>(monitoringWindowStartTime - baselineOffsetMillis, monitoringWindowEndTime - baselineOffsetMillis));

    return startEndTimeIntervals;
  }

  /**
   * Given any metric, this method returns the corresponding current and baseline time series to be presented in the
   * frontend. The given metric is not necessary the data that is used for detecting anomaly.
   *
   * For instance, if a function uses the average values of the past 3 weeks as the baseline for anomaly detection,
   * then this method should construct a baseline that contains the average value of the past 3 weeks of the given
   * metric.
   *
   * By default, this method returns the values in the previous week as baseline time series and ignores the
   * information of known anomalies.
   *
   * Note that the usage of this method should be similar to the method {@link #analyze}, i.e., it does not take care
   * of setting filters, dimension names, etc. for retrieving the data from the backend database. Specifically, it only
   * processes the given data, i.e., timeSeries, for presentation purpose.
   *
   * The only difference between this method and {@link #analyze} is that their bucket sizes are different.
   * This method's bucket size is given by frontend, which should larger or equal to the minimum time granularity of
   * the data. On the other hand, {@link #analyze}'s buckets size is always the minimum time granularity of the data.
   *
   * @param timeSeries the time series that contains the metric to be processed
   * @param metric the metric name to retrieve the data from the given time series
   * @param bucketMillis the size of a bucket in milli-seconds
   * @param viewWindowStartTime the start time bucket of current time series, inclusive
   * @param viewWindowEndTime the end time buckets of current time series, exclusive
   * @return Two set of time series: a current and a baseline values, to be represented in the frontend
   */
  public AnomalyTimelinesView getPresentationTimeseries(MetricTimeSeries timeSeries, long bucketMillis, String metric,
      long viewWindowStartTime, long viewWindowEndTime, List<RawAnomalyResultDTO> knownAnomalies) {

    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();

    // Construct AnomalyTimelinesView
    int bucketCount = (int) ((viewWindowEndTime - viewWindowStartTime) / bucketMillis);
    for (int i = 0; i < bucketCount; ++i) {
      long currentBucketMillis = viewWindowStartTime + i * bucketMillis;
      long baselineBucketMillis = currentBucketMillis - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
      TimeBucket timebucket =
          new TimeBucket(currentBucketMillis, currentBucketMillis + bucketMillis, baselineBucketMillis,
              baselineBucketMillis + bucketMillis);
      anomalyTimelinesView.addTimeBuckets(timebucket);
      anomalyTimelinesView.addCurrentValues(timeSeries.get(currentBucketMillis, metric).doubleValue());
      anomalyTimelinesView.addBaselineValues(timeSeries.get(baselineBucketMillis, metric).doubleValue());
    }

    return anomalyTimelinesView;
  }

}
