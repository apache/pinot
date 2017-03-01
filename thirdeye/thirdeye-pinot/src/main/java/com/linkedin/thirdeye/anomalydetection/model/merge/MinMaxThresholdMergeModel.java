package com.linkedin.thirdeye.anomalydetection.model.merge;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.model.detection.MinMaxThresholdDetectionModel;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Properties;
import org.joda.time.Interval;

public class MinMaxThresholdMergeModel extends AbstractMergeModel implements NoPredictionMergeModel {
  public static final String DEFAULT_MESSAGE_TEMPLATE = "change : %.2f %%, currentVal : %.2f, min : %.2f, max : %.2f";
  public static final String MIN_VAL = "min";
  public static final String MAX_VAL = "max";

  @Override public void update(AnomalyDetectionContext anomalyDetectionContext,
      MergedAnomalyResultDTO anomalyToUpdated) {
    // Get min / max props
    Properties props = getProperties();
    Double min = null;
    if (props.containsKey(MIN_VAL)) {
      min = Double.valueOf(props.getProperty(MIN_VAL));
    }
    Double max = null;
    if (props.containsKey(MAX_VAL)) {
      max = Double.valueOf(props.getProperty(MAX_VAL));
    }

    String metricName =
        anomalyDetectionContext.getAnomalyDetectionFunction().getSpec().getTopicMetric();
    TimeSeries timeSeries = anomalyDetectionContext.getTransformedCurrent(metricName);
    Interval timeSeriesInterval = timeSeries.getTimeSeriesInterval();

    long windowStartInMillis = timeSeriesInterval.getStartMillis();
    long windowEndInMillis = timeSeriesInterval.getEndMillis();

    double currentAverageValue = 0d;
    int currentBucketCount = 0;
    double deviationFromThreshold = 0d;
    for (long time : timeSeries.timestampSet()) {
      double value = timeSeries.get(time);
      if (value != 0d) {
        if (windowStartInMillis <= time && time <= windowEndInMillis) {
          currentAverageValue += value;
          ++currentBucketCount;
          deviationFromThreshold += MinMaxThresholdDetectionModel.getDeviationFromThreshold(value, min, max);
        } // else ignore unknown time key
      }
    }

    if (currentBucketCount != 0d) {
      currentAverageValue /= currentBucketCount;
      deviationFromThreshold /= currentBucketCount;
    }
    anomalyToUpdated.setScore(currentAverageValue);
    anomalyToUpdated.setWeight(deviationFromThreshold);
    anomalyToUpdated.setAvgCurrentVal(currentAverageValue);

    String message =
        String.format(DEFAULT_MESSAGE_TEMPLATE, deviationFromThreshold, currentAverageValue, min, max);
    anomalyToUpdated.setMessage(message);
  }
}
