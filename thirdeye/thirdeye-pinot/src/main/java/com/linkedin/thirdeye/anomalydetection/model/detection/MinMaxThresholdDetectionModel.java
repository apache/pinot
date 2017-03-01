package com.linkedin.thirdeye.anomalydetection.model.detection;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Interval;

public class MinMaxThresholdDetectionModel extends AbstractDetectionModel {
  public static final String DEFAULT_MESSAGE_TEMPLATE = "change : %.2f %%, currentVal : %.2f, min : %.2f, max : %.2f";
  public static final String MIN_VAL = "min";
  public static final String MAX_VAL = "max";

  @Override
  public List<RawAnomalyResultDTO> detect(String metricName,
      AnomalyDetectionContext anomalyDetectionContext) {
    List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();

    // Get min / max props
    Double min = null;
    if (properties.containsKey(MIN_VAL)) {
      min = Double.valueOf(properties.getProperty(MIN_VAL));
    }
    Double max = null;
    if (properties.containsKey(MAX_VAL)) {
      max = Double.valueOf(properties.getProperty(MAX_VAL));
    }

    TimeSeries timeSeries = anomalyDetectionContext.getTransformedCurrent(metricName);

    // Compute the weight of this time series (average across whole)
    double averageValue = 0;
    for (long time : timeSeries.timestampSet()) {
      averageValue += timeSeries.get(time);
    }
    // Compute the bucket size, so we can iterate in those steps
    long bucketMillis = anomalyDetectionContext.getBucketSizeInMS();
    Interval timeSeriesInterval = timeSeries.getTimeSeriesInterval();
    long numBuckets = Math.abs(timeSeriesInterval.getEndMillis() - timeSeriesInterval.getStartMillis()) / bucketMillis;

    // avg value of this time series
    averageValue /= numBuckets;

    DimensionMap dimensionMap = anomalyDetectionContext.getTimeSeriesKey().getDimensionMap();
    for (long timeBucket : timeSeries.timestampSet()) {
      double value = timeSeries.get(timeBucket);
      double deviationFromThreshold = getDeviationFromThreshold(value, min, max);

      if (deviationFromThreshold != 0) {
        RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
        anomalyResult.setProperties(properties.toString());
        anomalyResult.setStartTime(timeBucket);
        anomalyResult.setEndTime(timeBucket + bucketMillis); // point-in-time
        anomalyResult.setDimensions(dimensionMap);
        anomalyResult.setScore(averageValue);
        anomalyResult.setWeight(deviationFromThreshold); // higher change, higher the severity
        anomalyResult.setAvgCurrentVal(value);
        String message =
            String.format(DEFAULT_MESSAGE_TEMPLATE, deviationFromThreshold, value, min, max);
        anomalyResult.setMessage(message);
        if (value == 0.0) {
          anomalyResult.setDataMissing(true);
        }
        anomalyResults.add(anomalyResult);
      }
    }

    return anomalyResults;
  }

  public static double getDeviationFromThreshold(double currentValue, Double min, Double max) {
    if ((min != null && currentValue < min && min != 0d)) {
      return calculateChange(currentValue, min);
    } else if (max != null && currentValue > max && max != 0d) {
      return calculateChange(currentValue, max);
    }
    return 0;
  }

  protected static double calculateChange(double currentValue, double baselineValue) {
    return (currentValue - baselineValue) / baselineValue;
  }
}
