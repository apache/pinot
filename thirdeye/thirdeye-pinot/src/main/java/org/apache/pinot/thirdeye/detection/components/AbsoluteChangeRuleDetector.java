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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.Pattern;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.annotation.PresentationOption;
import org.apache.pinot.thirdeye.detection.spec.AbsoluteChangeRuleDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import org.joda.time.Interval;

import static org.apache.pinot.thirdeye.dataframe.DoubleSeries.*;

@Components(title = "Absolute change rule detection",
    type = "ABSOLUTE_CHANGE_RULE",
    tags = {DetectionTag.RULE_DETECTION},
    presentation = {
        @PresentationOption(name = "absolute value", template = "comparing ${offset} is ${pattern} more than ${difference}"),
    },
    params = {
        @Param(name = "offset", defaultValue = "wo1w"),
        @Param(name = "change", placeholder = "value"),
        @Param(name = "pattern", allowableValues = {"up", "down"})
    })
public class AbsoluteChangeRuleDetector implements AnomalyDetector<AbsoluteChangeRuleDetectorSpec>,
                                                   BaselineProvider<AbsoluteChangeRuleDetectorSpec> {
  private double absoluteChange;
  private InputDataFetcher dataFetcher;
  private Baseline baseline;
  private Pattern pattern;
  private String monitoringGranularity;
  private TimeGranularity timeGranularity;

  private static final String COL_CURR = "current";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_DIFF = "diff";
  private static final String COL_PATTERN = "pattern";
  private static final String COL_DIFF_VIOLATION = "diff_violation";

  @Override
  public DetectionResult runDetection(Interval window, String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    MetricSlice slice = MetricSlice.from(me.getId(), window.getStartMillis(), window.getEndMillis(), me.getFilters(), timeGranularity);
    List<MetricSlice> slices = new ArrayList<>(this.baseline.scatter(slice));
    slices.add(slice);
    InputData data = this.dataFetcher.fetchData(new InputDataSpec().withTimeseriesSlices(slices).withMetricIdsForDataset(
        Collections.singletonList(slice.getMetricId())));
    DataFrame dfCurr = data.getTimeseries().get(slice).renameSeries(DataFrame.COL_VALUE, COL_CURR);
    DataFrame dfBase = this.baseline.gather(slice, data.getTimeseries());

    DataFrame df = new DataFrame(dfCurr).addSeries(dfBase);
    // calculate absolute change
    df.addSeries(COL_DIFF, df.getDoubles(COL_CURR).subtract(df.get(DataFrame.COL_VALUE)));

    // defaults
    df.addSeries(COL_ANOMALY, BooleanSeries.fillValues(df.size(), false));
    // absolute change
    if (!Double.isNaN(this.absoluteChange)) {
      // consistent with pattern
      if (pattern.equals(Pattern.UP_OR_DOWN) ) {
        df.addSeries(COL_PATTERN, BooleanSeries.fillValues(df.size(), true));
      } else {
        df.addSeries(COL_PATTERN, this.pattern.equals(Pattern.UP) ? df.getDoubles(COL_DIFF).gt(0) : df.getDoubles(COL_DIFF).lt(0));
      }
      df.addSeries(COL_DIFF_VIOLATION, df.getDoubles(COL_DIFF).abs().gte(this.absoluteChange));
      df.mapInPlace(BooleanSeries.ALL_TRUE, COL_ANOMALY, COL_PATTERN, COL_DIFF_VIOLATION);
    }

    // make anomalies
    DatasetConfigDTO datasetConfig = data.getDatasetForMetricId().get(me.getId());
    List<MergedAnomalyResultDTO> anomalies = DetectionUtils.makeAnomalies(slice, df, COL_ANOMALY,
        DetectionUtils.getMonitoringGranularityPeriod(monitoringGranularity, datasetConfig), datasetConfig);
    DataFrame baselineWithBoundaries = constructAbsoluteChangeBoundaries(df);
    return DetectionResult.from(anomalies, TimeSeries.fromDataFrame(baselineWithBoundaries));
  }

  @Override
  public TimeSeries computePredictedTimeSeries(MetricSlice slice) {
    DataFrame df = RuleBaselineProvider.buildBaselines(slice, this.baseline, this.dataFetcher);
    return TimeSeries.fromDataFrame(constructAbsoluteChangeBoundaries(df));
  }

  private DataFrame constructAbsoluteChangeBoundaries(DataFrame dfBase) {
    if (!Double.isNaN(this.absoluteChange)) {
      switch (this.pattern) {
        case UP:
          fillAbsoluteChangeBound(dfBase, DataFrame.COL_UPPER_BOUND, this.absoluteChange);
          dfBase.addSeries(DataFrame.COL_LOWER_BOUND, DoubleSeries.zeros(dfBase.size()));
          break;
        case DOWN:
          dfBase.addSeries(
              DataFrame.COL_UPPER_BOUND, DoubleSeries.fillValues(dfBase.size(), POSITIVE_INFINITY));
          fillAbsoluteChangeBound(dfBase, DataFrame.COL_LOWER_BOUND, -this.absoluteChange);
          break;
        case UP_OR_DOWN:
          fillAbsoluteChangeBound(dfBase, DataFrame.COL_UPPER_BOUND, this.absoluteChange);
          fillAbsoluteChangeBound(dfBase, DataFrame.COL_LOWER_BOUND, -this.absoluteChange);
          break;
        default:
          throw new IllegalArgumentException();
      }
    }
    return dfBase;
  }

  private void fillAbsoluteChangeBound(DataFrame dfBase, String colBound, double change) {
    dfBase.addSeries(colBound, map((DoubleFunction) values -> values[0] + change, dfBase.getDoubles(
        DataFrame.COL_VALUE)));
  }
    @Override
  public void init(AbsoluteChangeRuleDetectorSpec spec, InputDataFetcher dataFetcher) {
    this.absoluteChange = spec.getAbsoluteChange();
    this.dataFetcher = dataFetcher;
    String timezone = spec.getTimezone();
    String offset = spec.getOffset();
    this.baseline = BaselineParsingUtils.parseOffset(offset, timezone);
    this.pattern = Pattern.valueOf(spec.getPattern().toUpperCase());

    this.monitoringGranularity = spec.getMonitoringGranularity();
    if (this.monitoringGranularity.equals("1_MONTHS")) {
      this.timeGranularity = MetricSlice.NATIVE_GRANULARITY;
    } else {
      this.timeGranularity = TimeGranularity.fromString(spec.getMonitoringGranularity());
    }

  }
}
