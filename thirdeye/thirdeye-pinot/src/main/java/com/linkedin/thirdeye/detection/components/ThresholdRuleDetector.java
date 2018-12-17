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
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DetectionUtils;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.annotation.Components;
import com.linkedin.thirdeye.detection.annotation.DetectionTag;
import com.linkedin.thirdeye.detection.annotation.Param;
import com.linkedin.thirdeye.detection.annotation.PresentationOption;
import com.linkedin.thirdeye.detection.spec.ThresholdRuleDetectorSpec;
import com.linkedin.thirdeye.detection.spi.components.AnomalyDetector;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.Collections;
import java.util.List;
import org.joda.time.Interval;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


@Components(title = "Threshold", type = "THRESHOLD", tags = {
    DetectionTag.RULE_DETECTION}, description = "Simple threshold rule algorithm with (optional) upper and lower bounds on a metric value.", presentation = {
    @PresentationOption(name = "absolute value", template = "is lower than ${min} or higher than ${max}")}, params = {
    @Param(name = "min", placeholder = "value"), @Param(name = "max", placeholder = "value")})
public class ThresholdRuleDetector implements AnomalyDetector<ThresholdRuleDetectorSpec> {
  private final String COL_TOO_HIGH = "tooHigh";
  private final String COL_TOO_LOW = "tooLow";
  private final String COL_ANOMALY = "anomaly";

  private double min;
  private double max;
  private InputDataFetcher dataFetcher;

  @Override
  public List<MergedAnomalyResultDTO> runDetection(Interval window, String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    Long endTime = window.getEndMillis();
    MetricSlice slice = MetricSlice.from(me.getId(), window.getStartMillis(), endTime, me.getFilters());

    InputData data = this.dataFetcher.fetchData(
        new InputDataSpec().withTimeseriesSlices(Collections.singletonList(slice))
            .withMetricIdsForDataset(Collections.singletonList(me.getId())));
    DataFrame df = data.getTimeseries().get(slice);

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

    DatasetConfigDTO datasetConfig = data.getDatasetForMetricId().get(me.getId());
    return DetectionUtils.makeAnomalies(slice, df, COL_ANOMALY, endTime, datasetConfig);
  }

  @Override
  public void init(ThresholdRuleDetectorSpec spec, InputDataFetcher dataFetcher) {
    this.min = spec.getMin();
    this.max = spec.getMax();
    this.dataFetcher = dataFetcher;
  }
}
