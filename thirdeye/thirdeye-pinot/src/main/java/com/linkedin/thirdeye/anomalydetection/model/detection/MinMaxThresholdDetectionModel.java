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
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Interval;

public class MinMaxThresholdDetectionModel extends AbstractDetectionModel {
  public static final String MIN_VAL = "min";
  public static final String MAX_VAL = "max";

  @Override
  public List<AnomalyResult> detect(String metricName,
      AnomalyDetectionContext anomalyDetectionContext) {
    List<AnomalyResult> anomalyResults = new ArrayList<>();

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
        AnomalyResult anomalyResult = new RawAnomalyResult();
        anomalyResult.setProperties(ThirdEyeUtils.propertiesToStringMap(getProperties()));
        anomalyResult.setStartTime(timeBucket);
        anomalyResult.setEndTime(timeBucket + bucketMillis); // point-in-time
        anomalyResult.setDimensions(dimensionMap);
        anomalyResult.setScore(averageValue);
        anomalyResult.setWeight(deviationFromThreshold); // higher change, higher the severity
        anomalyResult.setAvgCurrentVal(value);
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
