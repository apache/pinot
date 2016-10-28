package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class WeekOverAverageWeeksAnomalyTimeSeriesView extends BaseAnomalyTimeSeriesView {
  public static final String SEASONAL_SIZE = "seasonalSize";
  public static final String SEASONAL_UNIT = "seasonalUnit";
  public static final String BASELINE_SEASONAL_PERIOD = "baselineSeasonalPeriod";

  public static final String DEFAULT_SEASONAL_SIZE = "7";
  public static final String DEFAULT_SEASONAL_UNIT = "DAYS";
  public static final String DEFAULT_BASELINE_SEASONAL_PERIOD = "4";

  @Override
  public List<Pair<Long, Long>> getDataRangeIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    // Monitoring data (current values)
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime, monitoringWindowEndTime));

    // Compute time ranges for training data (baseline values)
    int baselineSeasonalPeriod =
        Integer.parseInt(properties.getProperty(BASELINE_SEASONAL_PERIOD, DEFAULT_BASELINE_SEASONAL_PERIOD));
    int seasonalSize = Integer.parseInt(properties.getProperty(SEASONAL_SIZE, DEFAULT_SEASONAL_SIZE));
    TimeUnit seasonalUnit = TimeUnit.valueOf(properties.getProperty(SEASONAL_UNIT, DEFAULT_SEASONAL_UNIT));
    long seasonalMillis = seasonalUnit.toMillis(seasonalSize);

    for (int period = 1; period <= baselineSeasonalPeriod; ++period) {
      long seasonalShiftTime = seasonalMillis * period;
      long baselineStartTime = monitoringWindowStartTime - seasonalShiftTime;
      long baselineEndTime = monitoringWindowEndTime - seasonalShiftTime;
      startEndTimeIntervals.add(new Pair<>(baselineStartTime, baselineEndTime));
    }
    return startEndTimeIntervals;
  }

  /**
   * The baseline of the returned time series is the average of the values in past weeks, which is defined by
   * baselineSeasonalPeriod.
   *
   * TODO: Remove known anomalies from the time series
   */
  @Override
  public AnomalyTimelinesView getTimeSeriesView(MetricTimeSeries timeSeries, long bucketMillis, String metric,
      long viewWindowStartTime, long viewWindowEndTime, List<RawAnomalyResultDTO> knownAnomalies) {

    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();

    int baselineSeasonalPeriod =
        Integer.parseInt(properties.getProperty(BASELINE_SEASONAL_PERIOD, DEFAULT_BASELINE_SEASONAL_PERIOD));
    int seasonalSize = Integer.parseInt(properties.getProperty(SEASONAL_SIZE, DEFAULT_SEASONAL_SIZE));
    TimeUnit seasonalUnit = TimeUnit.valueOf(properties.getProperty(SEASONAL_UNIT, DEFAULT_SEASONAL_UNIT));
    long seasonalMillis = TimeUnit.MILLISECONDS.convert(seasonalSize, seasonalUnit);

    // Construct AnomalyTimelinesView
    int bucketCount = (int) ((viewWindowEndTime - viewWindowStartTime) / bucketMillis);
    for (int i = 0; i < bucketCount; ++i) {
      long currentBucketStart = viewWindowStartTime + i * bucketMillis;
      double currentValue = timeSeries.get(currentBucketStart, metric).doubleValue();
      anomalyTimelinesView.addCurrentValues(currentValue);

      double baselineAverageValue = 0;
      int baselineBucketCount = 0;
      for (int period = 1; period <= baselineSeasonalPeriod; ++period) {
        long baselineBucketMillis = currentBucketStart - seasonalMillis * period;
        double newBaselineValue = timeSeries.get(baselineBucketMillis, metric).doubleValue();
        if (Double.compare(newBaselineValue, 0d) != 0) {
          baselineAverageValue += newBaselineValue;
          ++baselineBucketCount;
        }
      }
      if (baselineBucketCount != 0) {
        baselineAverageValue /= baselineBucketCount;
      }
      anomalyTimelinesView.addBaselineValues(baselineAverageValue);

      long baselineBucketStart = currentBucketStart - seasonalMillis;
      long baselineBucketEnd = baselineBucketStart + bucketMillis;
      TimeBucket timebucket =
          new TimeBucket(currentBucketStart, currentBucketStart + bucketMillis, baselineBucketStart, baselineBucketEnd);
      anomalyTimelinesView.addTimeBuckets(timebucket);
    }

    return anomalyTimelinesView;
  }
}
