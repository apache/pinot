package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import java.util.List;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


public class TimeSeriesUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesUtils.class);

  /**
   * Convert the give observed and expected time series to anomaly time lines view
   * @param observed
   * @param expected
   * @param bucketMillis
   * @return
   */
  public static AnomalyTimelinesView toAnomalyTimeLinesView(TimeSeries observed, TimeSeries expected, long bucketMillis) {
    if (!observed.timestampSet().equals(expected.timestampSet())) {
      LOG.error("The timestamps of observed and expected are not the same");
      return null;
    }
    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();
    for (long timestamp : observed.timestampSet()) {
      TimeBucket timeBucket = new TimeBucket(timestamp, timestamp +  bucketMillis,
          timestamp, timestamp + bucketMillis);
      anomalyTimelinesView.addTimeBuckets(timeBucket);
      anomalyTimelinesView.addCurrentValues(observed.get(timestamp));
      anomalyTimelinesView.addBaselineValues(expected.get(timestamp));
    }
    return anomalyTimelinesView;
  }

  /**
   * Convert the given anomaly time lines view to a tuple of TimeSeries, the first one is observed, the second one is expected
   * @param anomalyTimelinesView
   * @return
   */
  public static Tuple2<TimeSeries, TimeSeries> toTimeSeries(AnomalyTimelinesView anomalyTimelinesView) {
    TimeSeries observed = new TimeSeries();
    TimeSeries expected = new TimeSeries();

    List<TimeBucket> timeBuckets = anomalyTimelinesView.getTimeBuckets();
    List<Double> observedValues = anomalyTimelinesView.getCurrentValues();
    List<Double> expectedValues = anomalyTimelinesView.getBaselineValues();
    for (int i = 0; i < timeBuckets.size(); i++) {
      observed.set(timeBuckets.get(i).getCurrentStart(), observedValues.get(i));
      expected.set(timeBuckets.get(i).getCurrentStart(), expectedValues.get(i));
    }
    observed.setTimeSeriesInterval(
        new Interval(timeBuckets.get(0).getCurrentStart(), timeBuckets.get(timeBuckets.size()- 1).getCurrentEnd()));
    expected.setTimeSeriesInterval(
        new Interval(timeBuckets.get(0).getCurrentStart(), timeBuckets.get(timeBuckets.size()- 1).getCurrentEnd()));
    return new Tuple2<>(observed, expected);
  }
}
