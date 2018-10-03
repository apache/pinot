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

package com.linkedin.thirdeye.detector.metric.transfer;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricTransfer {
  private static final Logger LOG = LoggerFactory.getLogger(MetricTransfer.class);
  private static final Double NULL_DOUBLE = Double.NaN;
  private static final boolean DEBUG = true;

  public static final String SEASONAL_SIZE = "seasonalSize";
  public static final String SEASONAL_UNIT = "seasonalUnit";
  public static final String BASELINE_SEASONAL_PERIOD = "baselineSeasonalPeriod";

  public static final String DEFAULT_SEASONAL_SIZE = "7";
  public static final String DEFAULT_SEASONAL_UNIT = "DAYS";
  public static final String DEFAULT_BASELINE_SEASONAL_PERIOD = "3";

  /**
   * Use the scaling factor to normalize the input time series
   *
   * @param metricsToModify the data MetricTimeSeries to be modified by scaling factor
   * @param windowStartTime the timestamp when the data belongs to current monitoring window
   * @param scalingFactorList list of scaling factors
   * @param metricName the metricName withing the timeseries to be modified
   * @param properties the properties for scaling the values
   */
  public static void rescaleMetric(MetricTimeSeries metricsToModify, long windowStartTime,
      List<ScalingFactor> scalingFactorList, String metricName, Properties properties) {
    if (CollectionUtils.isEmpty(scalingFactorList)) {
      return;  // no transformation if there is no scaling factor in
    }

    List<ScaledValue> scaledValues = null;
    if (DEBUG) {
      scaledValues = new ArrayList<>();
    }

    int seasonalSize = Integer.parseInt(properties.getProperty(SEASONAL_SIZE, DEFAULT_SEASONAL_SIZE));
    TimeUnit seasonalUnit = TimeUnit.valueOf(properties.getProperty(SEASONAL_UNIT, DEFAULT_SEASONAL_UNIT));
    long seasonalMillis = seasonalUnit.toMillis(seasonalSize);
    int baselineSeasonalPeriod = Integer.parseInt(properties.getProperty(BASELINE_SEASONAL_PERIOD, DEFAULT_BASELINE_SEASONAL_PERIOD));

    // The scaling model works as follows:
    // 1. If the time range of scaling factor overlaps with current time series, then the scaling
    //    factor is applied to all corresponding baseline time series.
    // 2. If the time range of scaling factor overlaps with a baseline time series, then baseline
    //    value that overlaps with the scaling factor is set to 0. Note that in current
    //    implementation of ThirdEye value 0 means missing data. Thus, the baseline value will be
    //    removed from baseline calculation.
    //
    // The model is implemented as follows:
    // 1. Check if a timestamp located in any time range of scaling factor.
    // 2. If it is, then check if it is a current value or baseline value.
    // 3a. If it is a baseline value, then set its value to 0.
    // 3b. If it is a current value, then apply the scaling factor to its corresponding baseline
    //     values.
    Set<Long> timeWindowSet = metricsToModify.getTimeWindowSet();
    for (long ts : timeWindowSet) {
      for (ScalingFactor sf: scalingFactorList) {
        if (sf.isInTimeWindow(ts)) {
          if (ts < windowStartTime) { // the timestamp belongs to a baseline time series and will be removed.
            if (DEBUG) {
              double originalValue = metricsToModify.getOrDefault(ts, metricName, NULL_DOUBLE).doubleValue();
              scaledValues.add(new ScaledValue(ts, originalValue, NULL_DOUBLE));
            }
            metricsToModify.set(ts, metricName, NULL_DOUBLE);
          } else { // the timestamp belongs to the current time series
            for (int i = 1; i <= baselineSeasonalPeriod; ++i) {
              long baseTs = ts - i * seasonalMillis;
              if (timeWindowSet.contains(baseTs)) {
                double originalValue = metricsToModify.getOrDefault(baseTs, metricName, NULL_DOUBLE).doubleValue();
                double scaledValue;
                // If original or scaled value is an empty value, then remove the timestamp from
                if (Double.compare(originalValue, NULL_DOUBLE) == 0
                    || Double.compare(sf.getScalingFactor(), NULL_DOUBLE) == 0) {
                  scaledValue = NULL_DOUBLE;
                } else {
                  scaledValue = originalValue * sf.getScalingFactor();
                }
                metricsToModify.set(baseTs, metricName, scaledValue);

                if (DEBUG) {
                  scaledValues.add(new ScaledValue(baseTs, originalValue, scaledValue));
                }
              }
            }
          }
        }
      }
    }

    if (DEBUG) {
      if (CollectionUtils.isNotEmpty(scaledValues)) {
        Collections.sort(scaledValues);
        StringBuilder sb = new StringBuilder();
        String separator = "";
        for (ScaledValue scaledValue : scaledValues) {
          sb.append(separator).append(scaledValue.toString());
          separator = ", ";
        }
        LOG.info("Transformed values: {}", sb.toString());
      }
    }
  }

  /**
   * This class is used to store debugging information, which is used to show the status of the
   * transformed time series.
   */
  private static class ScaledValue implements Comparable<ScaledValue> {
    long timestamp;
    double originalValue;
    double scaledValue;

    public ScaledValue(long timestamp, double originalValue, double scaledValue) {
      this.timestamp = timestamp;
      this.originalValue = originalValue;
      this.scaledValue = scaledValue;
    }

    /**
     * Used to sort ScaledValue by the natural order of their timestamp
     */
    @Override
    public int compareTo(ScaledValue o) {
      return Long.compare(timestamp, o.timestamp);
    }

    @Override
    public String toString() {
      return "ScaledValue{" + "time=" + timestamp + ", Value: " + originalValue
          + "->" + scaledValue + ", scale=" + scaledValue / originalValue + '}';
    }
  }
}
