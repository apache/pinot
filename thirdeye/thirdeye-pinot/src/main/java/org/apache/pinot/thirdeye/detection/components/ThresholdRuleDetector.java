/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection.components;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.annotation.PresentationOption;
import org.apache.pinot.thirdeye.detection.spec.ThresholdRuleDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.joda.time.Interval;

@Components(title = "Threshold", type = "THRESHOLD", tags = {
    DetectionTag.RULE_DETECTION}, description = "Simple threshold rule algorithm with (optional) upper and lower bounds on a metric value.", presentation = {
    @PresentationOption(name = "absolute value", template = "is lower than ${min} or higher than ${max}")}, params = {
    @Param(name = "min", placeholder = "value"), @Param(name = "max", placeholder = "value")})
public class ThresholdRuleDetector implements AnomalyDetector<ThresholdRuleDetectorSpec>, BaselineProvider<ThresholdRuleDetectorSpec> {
  private final String COL_TOO_HIGH = "tooHigh";
  private final String COL_TOO_LOW = "tooLow";
  private final String COL_ANOMALY = "anomaly";

  private double min;
  private double max;
  private InputDataFetcher dataFetcher;
  private String monitoringGranularity;
  private TimeGranularity timeGranularity;

  @Override
  public DetectionResult runDetection(Interval window, String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    Long endTime = window.getEndMillis();
    MetricSlice slice = MetricSlice.from(me.getId(), window.getStartMillis(), endTime, me.getFilters(), timeGranularity);

    InputData data = this.dataFetcher.fetchData(
        new InputDataSpec().withTimeseriesSlices(Collections.singletonList(slice))
            .withMetricIdsForDataset(Collections.singletonList(me.getId())));
    DataFrame df = data.getTimeseries().get(slice).renameSeries(
        DataFrame.COL_VALUE, DataFrame.COL_CURRENT);

    // defaults
    df.addSeries(COL_TOO_HIGH, BooleanSeries.fillValues(df.size(), false));
    df.addSeries(COL_TOO_LOW, BooleanSeries.fillValues(df.size(), false));

    // max
    if (!Double.isNaN(this.max)) {
      df.addSeries(COL_TOO_HIGH, df.getDoubles(DataFrame.COL_CURRENT).gt(this.max));
    }

    // min
    if (!Double.isNaN(this.min)) {
      df.addSeries(COL_TOO_LOW, df.getDoubles(DataFrame.COL_CURRENT).lt(this.min));
    }
    df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_TOO_HIGH, COL_TOO_LOW);
    DatasetConfigDTO datasetConfig = data.getDatasetForMetricId().get(me.getId());
    List<MergedAnomalyResultDTO> anomalies = DetectionUtils.makeAnomalies(slice, df, COL_ANOMALY,
        DetectionUtils.getMonitoringGranularityPeriod(monitoringGranularity, datasetConfig), datasetConfig);
    DataFrame baselineWithBoundaries = constructBaselineAndBoundaries(df);

    return DetectionResult.from(anomalies, TimeSeries.fromDataFrame(baselineWithBoundaries));
  }

  @Override
  public TimeSeries computePredictedTimeSeries(MetricSlice slice) {
    InputData data =
        this.dataFetcher.fetchData(new InputDataSpec().withTimeseriesSlices(Collections.singletonList(slice)));
    DataFrame df = data.getTimeseries().get(slice);
    return TimeSeries.fromDataFrame(constructBaselineAndBoundaries(df));
  }

  /**
   * Populate the dataframe with upper/lower boundaries and baseline
   */
  private DataFrame constructBaselineAndBoundaries(DataFrame df) {
    // Set default baseline as the actual value
    df.addSeries(DataFrame.COL_VALUE, df.get(DataFrame.COL_CURRENT));
    if (!Double.isNaN(this.min)) {
      df.addSeries(DataFrame.COL_LOWER_BOUND, DoubleSeries.fillValues(df.size(), this.min));
      // set baseline value as the lower bound when actual value across below the mark
      df.mapInPlace(DoubleSeries.MAX, DataFrame.COL_VALUE, DataFrame.COL_LOWER_BOUND, DataFrame.COL_VALUE);
    }
    if (!Double.isNaN(this.max)) {
      df.addSeries(DataFrame.COL_UPPER_BOUND, DoubleSeries.fillValues(df.size(), this.max));
      // set baseline value as the upper bound when actual value across above the mark
      df.mapInPlace(DoubleSeries.MIN, DataFrame.COL_VALUE, DataFrame.COL_UPPER_BOUND, DataFrame.COL_VALUE);
    }
    return df;
  }

  @Override
  public void init(ThresholdRuleDetectorSpec spec, InputDataFetcher dataFetcher) {
    this.min = spec.getMin();
    this.max = spec.getMax();
    this.dataFetcher = dataFetcher;
    this.monitoringGranularity = spec.getMonitoringGranularity();
    if (this.monitoringGranularity.equals("1_MONTHS")) {
      this.timeGranularity = MetricSlice.NATIVE_GRANULARITY;
    } else {
      this.timeGranularity = TimeGranularity.fromString(spec.getMonitoringGranularity());
    }
  }
}
