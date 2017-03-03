package com.linkedin.thirdeye.detector.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseAnomalyFunction implements AnomalyFunction {

  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  protected AnomalyFunctionDTO spec;

  @Override
  public void init(AnomalyFunctionDTO spec) throws Exception {
    this.spec = spec;
  }

  @Override
  public AnomalyFunctionDTO getSpec() {
    return spec;
  }

  public Properties getProperties() throws IOException {
    Properties props = new Properties();
    if (spec.getProperties() != null) {
      String[] tokens = spec.getProperties().split(";");
      for (String token : tokens) {
        props.load(new ByteArrayInputStream(token.getBytes()));
      }
    }
    return props;
  }

  @Override
  public List<Pair<Long, Long>> getDataRangeIntervals(Long monitoringWindowStartTime,
      Long monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime, monitoringWindowEndTime));
    return startEndTimeIntervals;
  }

  /**
   * This method provides a view of current time series, i.e., no baseline time series.
   *
   * @param timeSeries the time series that contains the metric to be processed
   * @param bucketMillis the size of a bucket in milli-seconds
   * @param metric the metric name to retrieve the data from the given time series
   * @param viewWindowStartTime the start time bucket of current time series, inclusive
   * @param viewWindowEndTime the end time buckets of current time series, exclusive
   * @param knownAnomalies it is assumed to be null for presentational purpose.
   * @return
   */
  @Override
  public AnomalyTimelinesView getTimeSeriesView(MetricTimeSeries timeSeries, long bucketMillis,
      String metric, long viewWindowStartTime, long viewWindowEndTime,
      List<RawAnomalyResultDTO> knownAnomalies) {

    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();

    // Construct Week-over-Week AnomalyTimelinesView
    int bucketCount = (int) ((viewWindowEndTime - viewWindowStartTime) / bucketMillis);
    for (int i = 0; i < bucketCount; ++i) {
      long currentBucketMillis = viewWindowStartTime + i * bucketMillis;
      long baselineBucketMillis = currentBucketMillis - TimeUnit.DAYS.toMillis(7);
      TimeBucket timebucket =
          new TimeBucket(currentBucketMillis, currentBucketMillis + bucketMillis, baselineBucketMillis,
              baselineBucketMillis + bucketMillis);
      anomalyTimelinesView.addTimeBuckets(timebucket);
      anomalyTimelinesView.addCurrentValues(timeSeries.get(currentBucketMillis, metric).doubleValue());
      anomalyTimelinesView.addBaselineValues(timeSeries.get(baselineBucketMillis, metric).doubleValue());
    }

    return anomalyTimelinesView;
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
   * Returns true if this anomaly function uses the information of history anomalies
   * @return true if this anomaly function uses the information of history anomalies
   */
  public boolean useHistoryAnomaly() {
    return false;
  }
}
