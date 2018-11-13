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

package com.linkedin.thirdeye.detection.algorithm.stage;

import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DefaultInputDataFetcher;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Static Anomaly detection stage. High level interface for anomaly detection stage.
 */
public abstract class StaticAnomalyDetectionStage implements AnomalyDetectionStage {
  private DataProvider provider;
  private long configId;

  @Override
  public void init(Map<String, Object> specs, Long configId, long startTime, long endTime) {
    this.configId = configId;
  }

  /**
   * Returns a data spec describing all required data(time series, aggregates, existing anomalies) to perform a stage.
   * Data is retrieved in one pass and cached between executions if possible.
   * @return input data spec
   */
  abstract InputDataSpec getInputDataSpec();

  /**
   * Run detection in the specified time range and return a list of anomalies
   * @param data data(time series, anomalies, etc.) as described by data spec
   * @return list of anomalies
   */
  abstract List<MergedAnomalyResultDTO> runDetection(InputData data);

  @Override
  public final List<MergedAnomalyResultDTO> runDetection(DataProvider provider) {
    this.provider = provider;
    InputDataFetcher dataFetcher = new DefaultInputDataFetcher(this.provider, this.configId);
    return this.runDetection(dataFetcher.fetchData(this.getInputDataSpec()));
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

  /**
   * Helper for creating a list of anomalies from a boolean series. Injects properties via
   * {@code makeAnomaly(MetricSlice, MetricConfigDTO, Long)}.
   *
   * @param slice metric slice
   * @param df time series with COL_TIME and at least one boolean value series
   * @param seriesName name of the value series
   * @param configId configuration id of this pipeline
   * @param endTime end time of this detection window
   * @return list of anomalies
   */
  protected final List<MergedAnomalyResultDTO> makeAnomalies(MetricSlice slice, DataFrame df, String seriesName, Long configId, long endTime) {
    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    df = df.filter(df.getLongs(COL_TIME).between(slice.getStart(), slice.getEnd())).dropNull(COL_TIME);

    if (df.isEmpty()) {
      return Collections.emptyList();
    }

    Map<Long, MetricConfigDTO> metrics = this.provider.fetchMetrics(Collections.singleton(slice.getMetricId()));
    if (!metrics.containsKey(slice.getMetricId())) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.getMetricId()));
    }

    MetricConfigDTO metric = metrics.get(slice.getMetricId());

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

      // guess-timate of next time series timestamp
      DatasetConfigDTO dataset = this.provider.fetchDatasets(Collections.singleton(metric.getDataset())).get(metric.getDataset());
      if (dataset != null) {
        Period period = dataset.bucketTimeGranularity().toPeriod();
        DateTimeZone timezone = DateTimeZone.forID(dataset.getTimezone());

        long lastTimestamp = sTime.getLong(sTime.size() - 1);

        end = new DateTime(lastTimestamp, timezone).plus(period).getMillis();
      }

      // truncate at analysis end time
      end = Math.min(end, endTime);

      anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end)));
    }

    return anomalies;
  }
}
