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

package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


public class TimeSeriesUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesUtils.class);

  /**
   * Convert the give observed and expected time series to anomaly time lines view
   * @param observed
   * @param expected
   * @param bucketMillis
   * @return
   */
  public static AnomalyTimelinesView toAnomalyTimeLinesView(TimeSeries observed, TimeSeries expected, long bucketMillis) {
    if (!observed.timestampSet().equals(expected.timestampSet())) {
      LOG.error("The timestamps of observed and expected are not the same");
      return null;
    }
    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();
    for (long timestamp : observed.timestampSet()) {
      TimeBucket timeBucket = new TimeBucket(timestamp, timestamp +  bucketMillis,
          timestamp, timestamp + bucketMillis);
      anomalyTimelinesView.addTimeBuckets(timeBucket);
      anomalyTimelinesView.addCurrentValues(observed.get(timestamp));
      anomalyTimelinesView.addBaselineValues(expected.get(timestamp));
    }
    return anomalyTimelinesView;
  }

  /**
   * Convert the given anomaly time lines view to a tuple of TimeSeries, the first one is observed, the second one is expected
   * @param anomalyTimelinesView
   * @return
   */
  public static Tuple2<TimeSeries, TimeSeries> toTimeSeries(AnomalyTimelinesView anomalyTimelinesView) {
    TimeSeries observed = new TimeSeries();
    TimeSeries expected = new TimeSeries();

    List<TimeBucket> timeBuckets = anomalyTimelinesView.getTimeBuckets();
    List<Double> observedValues = anomalyTimelinesView.getCurrentValues();
    List<Double> expectedValues = anomalyTimelinesView.getBaselineValues();
    TreeMap<TimeBucket, Tuple2<Double, Double>> sortedView = new TreeMap<>(new Comparator<TimeBucket>() {
      @Override
      public int compare(TimeBucket o1, TimeBucket o2) {
        return Long.compare(o1.getCurrentStart(), o2.getCurrentStart());
      }
    });
    for (int i = 0; i < timeBuckets.size(); i++) {
      sortedView.put(timeBuckets.get(i), new Tuple2<>(observedValues.get(i), expectedValues.get(i)));
    }

    timeBuckets = new ArrayList<>(sortedView.navigableKeySet());

    for (TimeBucket timeBucket : timeBuckets) {
      Tuple2<Double, Double> values = sortedView.get(timeBucket);
      observed.set(timeBucket.getCurrentStart(), values._1);
      expected.set(timeBucket.getCurrentStart(), values._2);
    }
    observed.setTimeSeriesInterval(
        new Interval(timeBuckets.get(0).getCurrentStart(), timeBuckets.get(timeBuckets.size()- 1).getCurrentEnd()));
    expected.setTimeSeriesInterval(
        new Interval(timeBuckets.get(0).getCurrentStart(), timeBuckets.get(timeBuckets.size()- 1).getCurrentEnd()));
    return new Tuple2<>(observed, expected);
  }
}
