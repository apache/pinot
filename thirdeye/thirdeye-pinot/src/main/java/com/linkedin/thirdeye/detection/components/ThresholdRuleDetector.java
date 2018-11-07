/*
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

package com.linkedin.thirdeye.detection.components;

import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.spec.ThresholdRuleDetectorSpec;
import com.linkedin.thirdeye.detection.spi.components.AnomalyDetector;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.joda.time.Interval;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class ThresholdRuleDetector implements AnomalyDetector<ThresholdRuleDetectorSpec> {
  private final String COL_TOO_HIGH = "tooHigh";
  private final String COL_TOO_LOW = "tooLow";
  private final String COL_ANOMALY = "anomaly";

  private double min;
  private double max;
  private MetricSlice slice;
  private Long configId;
  private Long endTime;
  private MetricEntity me;
  @Override
  public InputDataSpec getInputDataSpec(Interval interval, String metricUrn, long configId) {
    this.me = MetricEntity.fromURN(metricUrn);
    this.configId = configId;
    this.endTime = interval.getEndMillis();
    this.slice = MetricSlice.from(me.getId(), interval.getStartMillis(), endTime, me.getFilters());

    return new InputDataSpec()
        .withTimeseriesSlices(Collections.singletonList(this.slice));
  }

  @Override
  public List<MergedAnomalyResultDTO> runDetection(InputData data) {
    DataFrame df = data.getTimeseries().get(this.slice);

    // defaults
    df.addSeries(COL_TOO_HIGH, BooleanSeries.fillValues(df.size(), false));
    df.addSeries(COL_TOO_LOW, BooleanSeries.fillValues(df.size(), false));

    // max
    if (!Double.isNaN(this.max)) {
      df.addSeries(COL_TOO_HIGH, df.getDoubles(COL_VALUE).gt(this.max));
    }

    // min
    if (!Double.isNaN(this.min)) {
      df.addSeries(COL_TOO_LOW, df.getDoubles(COL_VALUE).lt(this.min));
    }

    df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_TOO_HIGH, COL_TOO_LOW);

    return this.makeAnomalies(this.slice, df, COL_ANOMALY, this.endTime);
  }

  @Override
  public void init(ThresholdRuleDetectorSpec spec) {
    this.min = spec.getMin();
    this.max = spec.getMax();
  }

  /**
   * Helper for creating a list of anomalies from a boolean series. Injects properties via
   * {@code makeAnomaly(MetricSlice, MetricConfigDTO, Long)}.
   *
   * @param slice metric slice
   * @param df time series with COL_TIME and at least one boolean value series
   * @param seriesName name of the value series
   * @param endTime end time of this detection window
   * @return list of anomalies
   */
  protected final List<MergedAnomalyResultDTO> makeAnomalies(MetricSlice slice, DataFrame df, String seriesName, long endTime) {
    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    df = df.filter(df.getLongs(COL_TIME).between(slice.getStart(), slice.getEnd())).dropNull(COL_TIME);

    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    LongSeries sTime = df.getLongs(COL_TIME);
    BooleanSeries sVal = df.getBooleans(seriesName);

    int lastStart = -1;
    for (int i = 0; i < df.size(); i++) {
      if (sVal.isNull(i) || !BooleanSeries.booleanValueOf(sVal.get(i))) {
        // end of a run
        if (lastStart >= 0) {
          long start = sTime.get(lastStart);
          long end = sTime.get(i);
          anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end)));
        }
        lastStart = -1;

      } else {
        // start of a run
        if (lastStart < 0) {
          lastStart = i;
        }
      }
    }

    // end of current run
    if (lastStart >= 0) {
      long start = sTime.get(lastStart);
      long end = start + 1;

      // truncate at analysis end time
      end = Math.min(end, endTime);

      anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end)));
    }

    return anomalies;
  }

  /**
   * Helper for creating an anomaly for a given metric slice. Injects properties such as
   * metric name, filter dimensions, etc.
   *
   * @param slice metric slice
   * @return anomaly template
   */
  protected final MergedAnomalyResultDTO makeAnomaly(MetricSlice slice) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(slice.getStart());
    anomaly.setEndTime(slice.getEnd());

    return anomaly;
  }



}
