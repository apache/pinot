package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WeekOverWeekAnomalyTimeSeriesView extends BaseAnomalyTimeSeriesView {
  private static final Logger LOG = LoggerFactory.getLogger(WeekOverWeekAnomalyTimeSeriesView.class);

  public static final String BASELINE = "baseline";

  long getBaselineOffsetMillis(String baselineProp) throws IllegalArgumentException {
    long baselineOffsetMillis;
    if ("w/w".equals(baselineProp)) {
      baselineOffsetMillis = TimeUnit.DAYS.toMillis(7);
    } else if ("w/2w".equals(baselineProp)) {
      baselineOffsetMillis = TimeUnit.DAYS.toMillis(14);
    } else if ("w/3w".equals(baselineProp)) {
      baselineOffsetMillis = TimeUnit.DAYS.toMillis(21);
    } else {
      throw new IllegalArgumentException("Unsupported baseline " + baselineProp);
    }
    return baselineOffsetMillis;
  }

  @Override
  public List<Pair<Long, Long>> getDataRangeIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime, monitoringWindowEndTime));

    long baselineMillis;
    try {
      // Compute baseline for comparison
      String baselineProp = properties.getProperty(BASELINE);
      baselineMillis = getBaselineOffsetMillis(baselineProp);
    } catch (Exception e) {
      LOG.info("Cannot get function property when constructing presentation timeseires. Using w/w baseline values.");
      baselineMillis = TimeUnit.DAYS.toMillis(7);
    }
    startEndTimeIntervals
        .add(new Pair<>(monitoringWindowStartTime - baselineMillis, monitoringWindowEndTime - baselineMillis));
    return startEndTimeIntervals;
  }

  @Override
  public AnomalyTimelinesView getTimeSeriesView(MetricTimeSeries timeSeries, long bucketMillis, String metric,
      long viewWindowStartTime, long viewWindowEndTime, List<RawAnomalyResultDTO> knownAnomalies) {
    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();

    // Compute baseline for comparison
    long baselineOffsetInMillis;
    try {
      String baselineProp = properties.getProperty(BASELINE);
      baselineOffsetInMillis = getBaselineOffsetMillis(baselineProp);
    } catch (IllegalArgumentException e) {
      LOG.info("Cannot get function property when constructing presentation timeseires. Using w/w baseline values.");
      baselineOffsetInMillis = TimeUnit.DAYS.toMillis(7);
    }

    // Construct AnomalyTimelinesView
    int bucketCount = (int) ((viewWindowEndTime - viewWindowStartTime) / bucketMillis);
    for (int i = 0; i < bucketCount; ++i) {
      long currentBucketMillis = viewWindowStartTime + i * bucketMillis;
      long baselineBucketMillis = currentBucketMillis - baselineOffsetInMillis;
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
