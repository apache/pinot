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
import java.util.concurrent.TimeUnit;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleThreshold extends AbstractDetectionModel {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleThreshold.class);

  public static final String CHANGE_THRESHOLD = "changeThreshold";
  public static final String AVERAGE_VOLUME_THRESHOLD = "averageVolumeThreshold";
  public static final String BUCKET_SIZE = "bucketSize";
  public static final String BUCKET_UNIT = "bucketUnit";

  public static final String DEFAULT_MESSAGE_TEMPLATE = "change : %.2f %%, currentVal : %.2f, baseLineVal : %.2f, threshold : %s, baseLineProp : %s";

  @Override
  public List<RawAnomalyResultDTO> detect(AnomalyDetectionContext anomalyDetectionContext) {
    List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();

    // Get thresholds
    double changeThreshold = Double.valueOf(getProperties().getProperty(CHANGE_THRESHOLD));
    double volumeThreshold = 0d;
    if (getProperties().containsKey(AVERAGE_VOLUME_THRESHOLD)) {
      volumeThreshold = Double.valueOf(getProperties().getProperty(AVERAGE_VOLUME_THRESHOLD));
    }

    // Calculate bucket size
    int bucketSize = Integer.valueOf(getProperties().getProperty(BUCKET_SIZE));
    TimeUnit bucketUnit = TimeUnit.valueOf(getProperties().getProperty(BUCKET_UNIT));
    long bucketSizeInMillis = bucketUnit.toMillis(bucketSize);

    // Compute the weight of this time series (average across whole)
    TimeSeries currentTimeSeries = anomalyDetectionContext.getTransformedCurrent();
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

    PredictionModel predictionModel = anomalyDetectionContext.getTrainedPredictionModel();
    if (!(predictionModel instanceof ExpectedTimeSeriesPredictionModel)) {
      LOGGER.info("SimpleThreshold detection model expects a ExpectedTimeSeriesPredictionModel but the trained prediction model in anomaly detection context is not.");
      return anomalyResults; // empty list
    }
    ExpectedTimeSeriesPredictionModel expectedTimeSeriesPredictionModel = (ExpectedTimeSeriesPredictionModel) predictionModel;

    TimeSeries expectedTimeSeries = expectedTimeSeriesPredictionModel.getExpectedTimeSeries();
    Interval expectedTSInterval = expectedTimeSeries.getTimeSeriesInterval();
    long expectedStart = expectedTSInterval.getStartMillis();
    long expectedEnd = expectedTSInterval.getEndMillis();
    for (int i = 0; i < numBuckets; ++i) {
      long offset = i * bucketSizeInMillis;
      long currentTimestamp = currentStart + offset;
      long expectedTimestamp = expectedStart + offset;
      if (expectedTimestamp >= expectedEnd) {
        break;
      }
      double currentValue = currentTimeSeries.get(currentTimestamp);
      double baselineValue = expectedTimeSeries.get(expectedTimestamp);
      if (isAnomaly(currentValue, baselineValue, changeThreshold)) {
        RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
        anomalyResult.setDimensions(dimensionMap);
        anomalyResult.setProperties(getProperties().toString());
        anomalyResult.setStartTime(currentTimestamp);
        anomalyResult.setEndTime(currentTimestamp + bucketSizeInMillis); // point-in-time
        anomalyResult.setScore(averageValue);
        anomalyResult.setWeight(calculateChange(currentValue, baselineValue));
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
