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

package com.linkedin.thirdeye.anomalydetection.model.prediction;

import com.linkedin.thirdeye.anomalydetection.AnomalyDetectionUtils;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.Interval;

public class SeasonalAveragePredictionModel extends ExpectedTimeSeriesPredictionModel {
  private TimeSeries expectedTimeSeries;

  @Override
  public void train(List<TimeSeries> baselineTimeSeries, AnomalyDetectionContext anomalyDetectionContext) {
    expectedTimeSeries = new TimeSeries();

    if (CollectionUtils.isNotEmpty(baselineTimeSeries)) {
      TimeSeries baseTimeSeries = getLatestTimeSeries(baselineTimeSeries);
      Interval baseInterval = baseTimeSeries.getTimeSeriesInterval();

      long bucketSizeInMillis = anomalyDetectionContext.getBucketSizeInMS();

      long baseStart = baseInterval.getStartMillis();
      long baseEnd = baseInterval.getEndMillis();
      int bucketCount = (int) ((baseEnd - baseStart) / bucketSizeInMillis);
      expectedTimeSeries.setTimeSeriesInterval(baseInterval);

      if (baselineTimeSeries.size() > 1) {
        for (int i = 0; i < bucketCount; ++i) {
          double sum = 0d;
          int count = 0;
          long timeOffset = i * bucketSizeInMillis;
          for (TimeSeries ts : baselineTimeSeries) {
            long timestamp = ts.getTimeSeriesInterval().getStartMillis() + timeOffset;
            Double value = ts.get(timestamp);
            if (value != null) {
              sum += value;
              ++count;
            }
          }
          if (count != 0) {
            long timestamp = baseStart + timeOffset;
            double avgValue = sum / (double) count;
            expectedTimeSeries.set(timestamp, avgValue);
          }
        }
      } else {
        for (int i = 0; i < bucketCount; ++i) {
          long timestamp = baseStart + i * bucketSizeInMillis;
          Double value = baseTimeSeries.get(timestamp);
          if (value != null) {
            expectedTimeSeries.set(timestamp, value);
          }
        }
      }
    }
  }

  @Override
  public TimeSeries getExpectedTimeSeries() {
    return expectedTimeSeries;
  }

  /**
   * Returns the time series, which has the largest start millis, from a set of time series.
   *
   * @param baselineTimeSeries the set of baselines
   * @return the time series, which has the largest start millis, from a set of time series.
   */
  private TimeSeries getLatestTimeSeries(List<TimeSeries> baselineTimeSeries) {
    if (CollectionUtils.isNotEmpty(baselineTimeSeries)) {
      if (baselineTimeSeries.size() > 1) {
        TimeSeries latestTimeSeries = baselineTimeSeries.get(0);
        Interval latestInterval = latestTimeSeries.getTimeSeriesInterval();
        for (TimeSeries ts : baselineTimeSeries) {
          Interval currentInterval = ts.getTimeSeriesInterval();
          if (latestInterval.getStartMillis() < currentInterval.getStartMillis()) {
            latestTimeSeries = ts;
            latestInterval = currentInterval;
          }
        }
        return latestTimeSeries;
      } else {
        return baselineTimeSeries.get(0);
      }
    } else {
      return null;
    }
  }
}
