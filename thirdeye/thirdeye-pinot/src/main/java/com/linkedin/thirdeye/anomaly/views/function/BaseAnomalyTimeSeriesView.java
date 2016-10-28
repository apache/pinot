package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseAnomalyTimeSeriesView implements com.linkedin.thirdeye.anomaly.views.function.AnomalyTimeSeriesView {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAnomalyTimeSeriesView.class);

  protected AnomalyFunctionDTO spec;
  protected Properties properties;

  @Override
  public void init(AnomalyFunctionDTO spec) {
    this.spec = spec;
    try {
      this.properties = AnomalyTimeSeriesViewUtils.getPropertiesFromSpec(spec);
    } catch (IOException e) {
      this.properties = new Properties();
      LOG.warn("Failed to read anomaly function spec from {}", spec.getFunctionName());
    }
  }

  @Override
  public AnomalyFunctionDTO getSpec() {
    return spec;
  }

  /**
   * Returns the data range intervals for showing week over week time series. The baseline is the data that 7 days
   * priors to current values.
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return the data range intervals for showing week over week time series
   */
  @Override
  public List<Pair<Long, Long>> getDataRangeIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime, monitoringWindowEndTime));

    long baselineOffsetMillis = TimeUnit.DAYS.toMillis(7);
    startEndTimeIntervals.add(
        new Pair<>(monitoringWindowStartTime - baselineOffsetMillis, monitoringWindowEndTime - baselineOffsetMillis));

    return startEndTimeIntervals;
  }

  /**
   * Returns the values in the previous week as baseline time series and ignores the information of known anomalies
   *
   * @param timeSeries the time series that contains the metric to be processed
   * @param metric the metric name to retrieve the data from the given time series
   * @param bucketMillis the size of a bucket in milli-seconds
   * @param viewWindowStartTime the start time bucket of current time series, inclusive
   * @param viewWindowEndTime the end time buckets of current time series, exclusive
   * @return Two set of time series: a current and a baseline values, to be represented in the frontend
   */
  public AnomalyTimelinesView getTimeSeriesView(MetricTimeSeries timeSeries, long bucketMillis, String metric,
      long viewWindowStartTime, long viewWindowEndTime, List<RawAnomalyResultDTO> knownAnomalies) {

    AnomalyTimelinesView
        anomalyTimelinesView = new AnomalyTimelinesView();

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
}
