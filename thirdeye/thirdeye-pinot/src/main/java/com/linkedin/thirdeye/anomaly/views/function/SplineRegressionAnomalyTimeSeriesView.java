package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.anomaly.api.AnomalyResult;
import com.linkedin.anomaly.api.MapMetricTimeSeries;
import com.linkedin.anomaly.lib.baseline.SplineRegressionBaselineData;
import com.linkedin.anomaly.util.IllegalFunctionException;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.anomaly.util.TimeRange;
import com.linkedin.thirdeye.controller.mp.function.SplineRegressionThirdEyeFunction;
import com.linkedin.thirdeye.dashboard.resources.OnboardResource;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.anomaly.api.SplineRegressionAnomalyDetectionFunction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SplineRegressionAnomalyTimeSeriesView extends BaseAnomalyTimeSeriesView{
  public static final String BUCKET_UNIT = "bucketUnit";
  public static final String HISTORICAL_DATA_LENGTH = "historicalDataLength";
  public static final String ANOMALY_REMOVAL_SEVERITY_THRESHOLD = "anomalyRemovalSeverityThreshold";
  public static final String TIMEZONE = "timezone";

  private static final String HISTORICAL_DATA_MAX_LENGTH = "historicalDataMaxLength";
  private static final String HISTORICAL_DATA_MIN_LENGTH = "historicalDataMinLength";
  private static final String DEGREE = "degree";
  private static final String P_VALUE_THRESHOLD = "pValueThreshold";
  private static final String LOG_TRANSFORM = "logTransform";

  /**
   * Default values of properties that are used in {@link com.linkedin.anomaly.api.SplineRegressionAnomalyDetectionFunction}.
   * These values have to be matched with those in {@link com.linkedin.anomaly.api.SplineRegressionAnomalyDetectionFunction}.
   */
  private static final String DEFAULT_DEGREE = "3";
  public static final String DEFAULT_BUCKET_UNIT = "DAYS";
  public static final String DEFAULT_HISTORICAL_DATA_LENGTH = "90";
  public static final String DEFAULT_HISTORICAL_DATA_MAX_LENGTH = "100";
  public static final String DEFAULT_HISTORICAL_DATA_MIN_LENGTH = "60";
  public static final String DEFAULT_TIMEZONE = "America/Los_Angeles";
  private static final String DEFAULT_ANOMALY_REMOVAL_SEVERITY_THRESHOLD = "0.5";
  private static final String DEFAULT_LOG_TRANSFORM = "False";
  private static final String DEFAULT_TIME_SERIES_LATENCY = "9";
  private static final long DAY_LENGTH = 86400000;
  private static final Logger LOG = LoggerFactory.getLogger(SplineRegressionAnomalyTimeSeriesView.class);

  @Override
  public List<Pair<Long, Long>> getDataRangeIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    // Monitoring data (current values)
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime, monitoringWindowEndTime));

    // Compute time ranges for training data (baseline values):
    // The baseline for SplineRegression is 2-3 months of continuous data.
    int historicalDataMaxLength = Integer.parseInt(
        properties.getProperty(HISTORICAL_DATA_LENGTH, DEFAULT_HISTORICAL_DATA_LENGTH));
    TimeUnit bucketUnit =
        TimeUnit.valueOf(properties.getProperty(BUCKET_UNIT, DEFAULT_BUCKET_UNIT));
    long trainingStartTimeMillis = 0L;
    // Daylight saving time only affects data ranges when TimeUnit is larger than HOURS. Since
    // there is only one unit, i.e., DAYS, that is larger than HOURS, it is sufficient to check
    // if user's unit equals to DAYS.
    if (TimeUnit.DAYS.equals(bucketUnit)) {
      String timeZoneString = properties.getProperty(TIMEZONE, DEFAULT_TIMEZONE);
      DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
      DateTime trainingStartTime = new DateTime(monitoringWindowStartTime, timeZone);
      Period period = new Period(0, 0, 0, historicalDataMaxLength, 0, 0, 0, 0);
      trainingStartTime = trainingStartTime.minus(period);
      trainingStartTimeMillis = trainingStartTime.getMillis();
    } else {
      long startOffset = bucketUnit.toMillis(historicalDataMaxLength);
      trainingStartTimeMillis = monitoringWindowStartTime - startOffset;
    }
    startEndTimeIntervals.add(new Pair<>(trainingStartTimeMillis, monitoringWindowStartTime));

    return startEndTimeIntervals;
  }

  /**
   * The baseline of the returned time series is the average of the values in past weeks, which is defined by
   * baselineSeasonalPeriod.
   *
   */
  @Override
  public AnomalyTimelinesView getTimeSeriesView(MetricTimeSeries timeSeries, long bucketMillis, String metric,
      long viewWindowStartTime, long viewWindowEndTime, List<RawAnomalyResultDTO> knownAnomalies) {

    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();

    SplineRegressionAnomalyDetectionFunction splineRegressionFunction =
        new SplineRegressionAnomalyDetectionFunction();
    try {
      splineRegressionFunction.init(properties);
    }
    catch (IllegalFunctionException e){
      LOG.error("{}: Unable to receive configure file", e.getMessage());
    }

    TimeRange monitorTimeRange = new TimeRange(viewWindowStartTime, viewWindowEndTime);

    List<AnomalyResult> anomalyHistory = new ArrayList<>();
    if(knownAnomalies != null && !knownAnomalies.isEmpty()) {
      for (RawAnomalyResultDTO anomaly : knownAnomalies) {
        AnomalyFeedbackDTO feedbackDTO = anomaly.getFeedback(); // null if no user feedback
        TimeRange anomalyTimeRange = new TimeRange(anomaly.getStartTime(), anomaly.getEndTime());
        AnomalyResult result = new AnomalyResult((feedbackDTO == null), anomalyTimeRange, anomaly.getScore(), Math.abs(anomaly.getWeight()), AnomalyResult.AnomalyMethod.SPLINEREGRESSION, properties);
        anomalyHistory.add(result);
      }
    }

    MapMetricTimeSeries series = new MapMetricTimeSeries();
    Set<Long> timeset =  timeSeries.getTimeWindowSet();
    List<Long> timelist = new ArrayList<>(timeset);
    Collections.sort(timelist);
    List<Long> timestamps = new ArrayList<>();
    List<Double> metricValue = new ArrayList<>();
    for(Long timestamp : timelist){
      timestamps.add(timestamp);
      metricValue.add(timeSeries.get(timestamp, metric).doubleValue());
    }

    SplineRegressionBaselineData baselineData = null;
    try {
      series = new MapMetricTimeSeries(timestamps, metricValue);
      baselineData = SplineRegressionAnomalyDetectionFunction.baselineEstimate(series, monitorTimeRange,
          anomalyHistory, bucketMillis,
          Double.valueOf(properties.getProperty(ANOMALY_REMOVAL_SEVERITY_THRESHOLD, DEFAULT_ANOMALY_REMOVAL_SEVERITY_THRESHOLD)),
          Integer.valueOf(properties.getProperty(HISTORICAL_DATA_MAX_LENGTH, DEFAULT_HISTORICAL_DATA_MAX_LENGTH)),
          Integer.valueOf(properties.getProperty(HISTORICAL_DATA_MIN_LENGTH, DEFAULT_HISTORICAL_DATA_MIN_LENGTH)),
          Integer.valueOf(properties.getProperty(DEGREE, DEFAULT_DEGREE)),
          Boolean.valueOf(properties.getProperty(LOG_TRANSFORM, DEFAULT_LOG_TRANSFORM)),
          properties.getProperty(TIMEZONE, DEFAULT_TIMEZONE));
    }
    catch(Exception e){
      LOG.error("{}", e.getMessage());
    }

    double[] observed = baselineData.getObserved();
    double[] expected = baselineData.getExpected();
    int begin_timestamp = timelist.size() - Integer.valueOf(DEFAULT_TIME_SERIES_LATENCY);
    begin_timestamp = Math.max(0, begin_timestamp);
    for(int i = begin_timestamp; i < timelist.size(); i++){
      long timestamp = timelist.get(i);
      TimeBucket timeBucket = new TimeBucket(timestamp, timestamp+DAY_LENGTH,
          timestamp, timestamp+DAY_LENGTH);
      anomalyTimelinesView.addTimeBuckets(timeBucket);
      anomalyTimelinesView.addCurrentValues(observed[i]);
      anomalyTimelinesView.addBaselineValues(expected[i]);
    }

    return anomalyTimelinesView;
  }

}
