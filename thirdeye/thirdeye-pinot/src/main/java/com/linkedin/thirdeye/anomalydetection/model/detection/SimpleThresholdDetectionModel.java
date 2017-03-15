package com.linkedin.thirdeye.anomalydetection.model.detection;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.model.prediction.ExpectedTimeSeriesPredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleThresholdDetectionModel extends AbstractDetectionModel {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleThresholdDetectionModel.class);

  public static final String CHANGE_THRESHOLD = "changeThreshold";
  public static final String AVERAGE_VOLUME_THRESHOLD = "averageVolumeThreshold";

  public static final String DEFAULT_MESSAGE_TEMPLATE = "change : %.2f %%, currentVal : %.2f, baseLineVal : %.2f, threshold : %s";

  @Override
  public List<RawAnomalyResultDTO> detect(String metricName, AnomalyDetectionContext anomalyDetectionContext) {
    List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();

    // Get thresholds
    double changeThreshold = Double.valueOf(getProperties().getProperty(CHANGE_THRESHOLD));
    double volumeThreshold = 0d;
    if (getProperties().containsKey(AVERAGE_VOLUME_THRESHOLD)) {
      volumeThreshold = Double.valueOf(getProperties().getProperty(AVERAGE_VOLUME_THRESHOLD));
    }

    long bucketSizeInMillis = anomalyDetectionContext.getBucketSizeInMS();

    // Compute the weight of this time series (average across whole)
    TimeSeries currentTimeSeries = anomalyDetectionContext.getTransformedCurrent(metricName);
    double averageValue = 0;
    for (long time : currentTimeSeries.timestampSet()) {
      averageValue += currentTimeSeries.get(time);
    }
    Interval currentInterval = currentTimeSeries.getTimeSeriesInterval();
    long currentStart = currentInterval.getStartMillis();
    long currentEnd = currentInterval.getEndMillis();
    long numBuckets = (currentEnd - currentStart) / bucketSizeInMillis;
    if (numBuckets != 0) {
      averageValue /= numBuckets;
    }

    // Check if this time series even meets our volume threshold
    DimensionMap dimensionMap = anomalyDetectionContext.getTimeSeriesKey().getDimensionMap();
    if (averageValue < volumeThreshold) {
      LOGGER.info("{} does not meet volume threshold {}: {}", dimensionMap, volumeThreshold, averageValue);
      return anomalyResults; // empty list
    }

    PredictionModel predictionModel = anomalyDetectionContext.getTrainedPredictionModel(metricName);
    if (!(predictionModel instanceof ExpectedTimeSeriesPredictionModel)) {
      LOGGER.info("SimpleThresholdDetectionModel detection model expects an ExpectedTimeSeriesPredictionModel but the trained prediction model in anomaly detection context is not.");
      return anomalyResults; // empty list
    }
    ExpectedTimeSeriesPredictionModel expectedTimeSeriesPredictionModel = (ExpectedTimeSeriesPredictionModel) predictionModel;

    TimeSeries expectedTimeSeries = expectedTimeSeriesPredictionModel.getExpectedTimeSeries();
    Interval expectedTSInterval = expectedTimeSeries.getTimeSeriesInterval();
    long expectedStart = expectedTSInterval.getStartMillis();
    long seasonalOffset = currentStart - expectedStart;
    for (long currentTimestamp : currentTimeSeries.timestampSet()) {
      long expectedTimestamp = currentTimestamp - seasonalOffset;
      if (!expectedTimeSeries.hasTimestamp(expectedTimestamp)) {
        continue;
      }
      double baselineValue = expectedTimeSeries.get(expectedTimestamp);
      double currentValue = currentTimeSeries.get(currentTimestamp);
      if (isAnomaly(currentValue, baselineValue, changeThreshold)) {
        RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
        anomalyResult.setDimensions(dimensionMap);
        anomalyResult.setProperties(getProperties().toString());
        anomalyResult.setStartTime(currentTimestamp);
        anomalyResult.setEndTime(currentTimestamp + bucketSizeInMillis); // point-in-time
        anomalyResult.setScore(averageValue);
        anomalyResult.setWeight(calculateChange(currentValue, baselineValue));
        anomalyResult.setAvgCurrentVal(currentValue);
        anomalyResult.setAvgBaselineVal(baselineValue);
        String message = getAnomalyResultMessage(changeThreshold, currentValue, baselineValue);
        anomalyResult.setMessage(message);
        anomalyResults.add(anomalyResult);
        if (currentValue == 0.0 || baselineValue == 0.0) {
          anomalyResult.setDataMissing(true);
        }
      }
    }

    return anomalyResults;
  }

  private boolean isAnomaly(double currentValue, double expectedValue, double threshold) {
    if (expectedValue > 0) {
      double percentChange = calculateChange(currentValue, expectedValue);
      if (threshold > 0 && percentChange > threshold || threshold < 0 && percentChange < threshold) {
        return true;
      }
    }
    return false;
  }

  private double calculateChange(double currentValue, double expectedValue) {
    return (currentValue - expectedValue) / expectedValue;
  }

  private String getAnomalyResultMessage(double threshold, double currentValue, double baselineValue) {
    double change = calculateChange(currentValue, baselineValue);
    NumberFormat percentInstance = NumberFormat.getPercentInstance();
    percentInstance.setMaximumFractionDigits(2);
    String thresholdPercent = percentInstance.format(threshold);
    String message = String
        .format(DEFAULT_MESSAGE_TEMPLATE, change * 100, currentValue, baselineValue, thresholdPercent);
    return message;
  }
}
