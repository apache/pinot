package com.linkedin.thirdeye.anomalydetection.model.transform;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.BooleanUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyRemovalFunction extends AbstractTransformationFunction {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AnomalyRemovalFunction.class);

  // timezone information is used here to denote the nature of the data not how it stored in database
  // only useful to get the offset period (to avoid the complication brought up by day light saving time)
  public static final String METRIC_TIMEZONE = "metricTimezone";
  public static final String DEFAULT_METRIC_TIMEZONE = "America/Los_Angeles";

  // if threshold is 0, then threshold is not used at all
  public static final String ANOMALY_REMOVAL_WEIGHT_THRESHOLD = "anomalyRemovalWeighThreshold";
  public static final String DEFAULT_ANOMALY_REMOVAL_WEIGHT_THRESHOLD = "0";

  // tolerance window, right before the current monitoring window, where we do not remove anomalies
  public static final String ANOMALY_REMOVAL_TOLERANCE_WINDOW_SIZE = "anomalyRemovalToleranceWindowSize";
  public static final String DEFAULT_ANOMALY_REMOVAL_TOLERANCE_WINDOW_SIZE = "0";


  /**
   * Remove historical anomalies region based on 'user label' and 'absolute weight'
   *
   * removal -- replace the value at the anomalous point as Double.NaN.
   *
   * @param timeSeries the time series that provides the data points to be transformed.
   * @param anomalyDetectionContext the anomaly detection context that could provide additional
   *                                information for the transformation.
   * @return a time series with anomalies removed.
   */
  @Override
  public TimeSeries transform(TimeSeries timeSeries, AnomalyDetectionContext anomalyDetectionContext) {
    Interval timeSeriesInterval = timeSeries.getTimeSeriesInterval();
    long startTime = timeSeriesInterval.getStartMillis();
    long endTime = timeSeriesInterval.getEndMillis();

    long bucketSizeInMillis = anomalyDetectionContext.getBucketSizeInMS();

    // get monitoring window
    String metricName = anomalyDetectionContext.getAnomalyDetectionFunction().getSpec().getTopicMetric();
    Interval currentWindow = anomalyDetectionContext.getCurrent(metricName).getTimeSeriesInterval();

    // get historical anomalies
    List<MergedAnomalyResultDTO> anomalyHistory = anomalyDetectionContext.getHistoricalAnomalies();

    double weightThreshold =
        Double.valueOf(getProperties().getProperty(ANOMALY_REMOVAL_WEIGHT_THRESHOLD, DEFAULT_ANOMALY_REMOVAL_WEIGHT_THRESHOLD));
    int toleranceSize =
        Integer.valueOf(getProperties().getProperty(ANOMALY_REMOVAL_TOLERANCE_WINDOW_SIZE, DEFAULT_ANOMALY_REMOVAL_TOLERANCE_WINDOW_SIZE));

    TimeSeries transformedTimeSeries = new TimeSeries();
    Interval transformedInterval = new Interval(startTime, endTime);
    transformedTimeSeries.setTimeSeriesInterval(transformedInterval);

    Map<Long, Boolean> anomalousTimestamps =
        getAnomalousTimeStampAndLabel(anomalyHistory, bucketSizeInMillis, weightThreshold);

    // get timestamp offset
    String metricTimezone = getProperties().getProperty(METRIC_TIMEZONE, DEFAULT_METRIC_TIMEZONE);
    long windowStartOffset =
        getOffsetTimestamp(currentWindow.getStartMillis(), toleranceSize, bucketSizeInMillis, metricTimezone);

    // generate the transformed time series
    for (long ts:timeSeries.timestampSet()) {
      Boolean userLabel = anomalousTimestamps.get(ts);
      userLabel = (userLabel == null) ? false : userLabel;
      if (ts < windowStartOffset || (userLabel && ts < currentWindow.getStartMillis())) {
        transformedTimeSeries.set(ts, Double.NaN);
      } else {
        transformedTimeSeries.set(ts, timeSeries.get(ts));
      }
    }

    return transformedTimeSeries;
  }

  /**
   * Extract the timestamps and their labels of historical anomalies if
   *  1) user labeled the anomaly as a true anomaly
   *  2) without label and the anomaly severity surpass the default weight (severity indicator) threshold
   *     this only works when a s
   * @param historicalAnomalies anomaly results in the past
   * @param bucketMillis the bucket in terms of milliseconds
   * @param weightThreshold the threshold to use on weight if user label is not given
   * @return the Map of timestamps for anomalies and their labels
   */
  public static Map<Long, Boolean> getAnomalousTimeStampAndLabel(
      List<MergedAnomalyResultDTO> historicalAnomalies, long bucketMillis, double weightThreshold) {

    Map<Long, Boolean> anomalousTimestamps = new HashMap<>();
    if (historicalAnomalies == null) {
      return anomalousTimestamps;  //empty map
    }
    for (MergedAnomalyResultDTO anomaly : historicalAnomalies) {
      boolean isRemovable = isRemovable(getUserLabel(anomaly), anomaly.getWeight(), weightThreshold);
      if (isRemovable) {
        int tsLength = (int) (1 + ((anomaly.getEndTime() - anomaly.getStartTime()) / bucketMillis));  // both ends included
        for (int i = 0; i < tsLength; i++) {
          anomalousTimestamps.put(anomaly.getStartTime() + i * bucketMillis, getUserLabel(anomaly));
        }
      }
    }
    return anomalousTimestamps;
  }

  /**
   * anomalous range is removable if
   *  1) labeled as true, or
   *  2) those not labeled but weight passed the weight threshold
   *
   */
  public static boolean isRemovable(Boolean isLabeledTrue, double weight, double weightThreshold) {
    return BooleanUtils.isTrue(isLabeledTrue)
        || (isLabeledTrue == null && weightThreshold >= 0 && Math.abs(weight) >= weightThreshold);
  }

  /**
   * translate user feedback type of a given anomaly to user label: 'null, true or false'
   *
   */
  public static Boolean getUserLabel(MergedAnomalyResultDTO anomalyResultDTO) {
    if (anomalyResultDTO == null) {
      return null;  // if input is null, return null
    }
    Boolean isUserLabeledAnomaly;
    AnomalyFeedbackType feedbackType;
    AnomalyFeedback feedback = anomalyResultDTO.getFeedback();
    if (feedback == null) {
      feedbackType = null;
    } else {
      feedbackType = feedback.getFeedbackType();
    }
    if (feedbackType == null || feedbackType.equals(AnomalyFeedbackType.NO_FEEDBACK)) {
      isUserLabeledAnomaly = null;
    } else {
      isUserLabeledAnomaly = !feedbackType.equals(AnomalyFeedbackType.NOT_ANOMALY);
    }
    return isUserLabeledAnomaly;
  }

  /**
   * get offset timestamp given current timestamp
   *
   * considering timezone information, solve daylight saving issue
   * @param currentTS current timestamp
   * @param offsetSize number of offset time unit
   * @param offsetUnit offset time unit offsetSize * offsetUnit = number of milliseconds to offset
   * @param timezone time zone we operated in
   * @return long
   */
  public static long getOffsetTimestamp(long currentTS, int offsetSize, long offsetUnit, String timezone) {
    // if no daylight saving issues in this timezone, should get direct offset
    // of if offsetUnit is not in days, then get direct offset
    if (timezone == null || offsetSize == 0 || offsetUnit == 0L
        || (offsetUnit % TimeUnit.DAYS.toMillis(1) != 0L)) {
      return currentTS - offsetSize * offsetUnit;
    }

    // let joda.time to compute the actual offset in milliseconds
    DateTimeZone dateTimeZone = DateTimeZone.forID(timezone);
    DateTime currentDT = new DateTime(currentTS, dateTimeZone);
    int bucketSizeInDays = (int) (offsetUnit / TimeUnit.DAYS.toMillis(1));
    Period offsetPeriod = new Period(0, 0, 0, offsetSize * bucketSizeInDays, 0, 0, 0, 0);

    return currentDT.minus(offsetPeriod).getMillis();
  }

}