package com.linkedin.thirdeye.detector.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.util.AnomalyOffset;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseAnomalyFunction implements AnomalyFunction {

  private static final TimeGranularity DEFAULT_VIEW_OFFSET_FOR_DAILY = new TimeGranularity(3, TimeUnit.DAYS);
  private static final TimeGranularity DEFAULT_VIEW_OFFSET_FOR_HOURLY = new TimeGranularity(10, TimeUnit.HOURS);
  private static final TimeGranularity DEFAULT_VIEW_OFFSET_FOR_MINUTE = new TimeGranularity(60, TimeUnit.MINUTES);
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

  @Override
  public List<RawAnomalyResultDTO> offlineAnalyze(DimensionMap exploredDimensions, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies) throws Exception {
    return Collections.emptyList();
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
      List<MergedAnomalyResultDTO> knownAnomalies) {

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
      anomalyTimelinesView.addCurrentValues(timeSeries.getOrDefault(currentBucketMillis, metric, 0).doubleValue());
      anomalyTimelinesView.addBaselineValues(timeSeries.getOrDefault(baselineBucketMillis, metric, 0).doubleValue());
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

  @Override
  public AnomalyOffset getAnomalyWindowOffset(DatasetConfigDTO datasetConfig) {
    // based on data granularity, decide offset
    AnomalyOffset anomalyWindowOffset = getDefaultOffsets(datasetConfig);
    return anomalyWindowOffset;
  }


  @Override
  public AnomalyOffset getViewWindowOffset(DatasetConfigDTO datasetConfig) {
    // based on data granularity, decide offset
    AnomalyOffset anomalyViewOffset = getDefaultOffsets(datasetConfig);
    return anomalyViewOffset;
  }

  private AnomalyOffset getDefaultOffsets(DatasetConfigDTO datasetConfig) {
    TimeUnit dataTimeUnit = datasetConfig.bucketTimeGranularity().getUnit();
    Period preOffsetPeriod = null;
    Period postOffsetPeriod = null;
    switch (dataTimeUnit) {
      case DAYS:
        preOffsetPeriod = DEFAULT_VIEW_OFFSET_FOR_DAILY.toPeriod();
        postOffsetPeriod = DEFAULT_VIEW_OFFSET_FOR_DAILY.toPeriod();
        break;
      case HOURS:
        preOffsetPeriod = DEFAULT_VIEW_OFFSET_FOR_HOURLY.toPeriod();
        postOffsetPeriod = DEFAULT_VIEW_OFFSET_FOR_HOURLY.toPeriod();
        break;
      case MINUTES:
        preOffsetPeriod = DEFAULT_VIEW_OFFSET_FOR_MINUTE.toPeriod();
        postOffsetPeriod = DEFAULT_VIEW_OFFSET_FOR_MINUTE.toPeriod();
        break;
      default:
        preOffsetPeriod = new Period();
        postOffsetPeriod = new Period();
    }
    AnomalyOffset anomalyOffset = new AnomalyOffset(preOffsetPeriod, postOffsetPeriod);
    return anomalyOffset;
  }

  /**
   * Specify the property keys used for merge comparison
   * Each anomaly detection function will be capable to overwrite this function to return the key lists when merging two anomalies
   * If two anomalies don't have equal value on the specified keys, they won't be merged
   * If this function is not being overwritten, empty list will be returned and anomalies won't be compared on mergeable keys when merging
   * @return A list of keys to used for comparing if two anomalies are equal on mergeable keys
   */
  public List<String> getMergeablePropertyKeys(){
    return Collections.emptyList();
  }

}
