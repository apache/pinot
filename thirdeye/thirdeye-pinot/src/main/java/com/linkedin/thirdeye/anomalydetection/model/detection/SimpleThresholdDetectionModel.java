/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomalydetection.model.detection;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.anomalydetection.context.RawAnomalyResult;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.model.prediction.ExpectedTimeSeriesPredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleThresholdDetectionModel extends AbstractDetectionModel {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleThresholdDetectionModel.class);

  public static final String CHANGE_THRESHOLD = "changeThreshold";
  public static final String AVERAGE_VOLUME_THRESHOLD = "averageVolumeThreshold";

  @Override
  public List<AnomalyResult> detect(String metricName, AnomalyDetectionContext anomalyDetectionContext) {
    List<AnomalyResult> anomalyResults = new ArrayList<>();

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
        AnomalyResult anomalyResult = new RawAnomalyResult();
        anomalyResult.setDimensions(dimensionMap);
        anomalyResult.setProperties(ThirdEyeUtils.propertiesToStringMap(getProperties()));
        anomalyResult.setStartTime(currentTimestamp);
        anomalyResult.setEndTime(currentTimestamp + bucketSizeInMillis); // point-in-time
        anomalyResult.setScore(averageValue);
        anomalyResult.setWeight(calculateChange(currentValue, baselineValue));
        anomalyResult.setAvgCurrentVal(currentValue);
        anomalyResult.setAvgBaselineVal(baselineValue);
        anomalyResults.add(anomalyResult);
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
}
