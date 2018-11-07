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
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.detection.annotation.Components;
import com.linkedin.thirdeye.detection.annotation.Param;
import com.linkedin.thirdeye.detection.annotation.DetectionTag;
import com.linkedin.thirdeye.detection.annotation.PresentationOption;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Simple threshold rule algorithm with (optional) upper and lower bounds on a metric value.
 */
@Components(title = "Threshold",
    type = "THRESHOLD",
    tags = { DetectionTag.RULE_DETECTION },
    description = "Simple threshold rule algorithm with (optional) upper and lower bounds on a metric value.",
    presentation = {@PresentationOption(
        name = "absolute value",
        description = "aggregated absolute value within a time period",
        template = "is lower than ${min} or higher than ${max}"
    )},
    params = {@Param(name = "min", placeholder = "value"), @Param(name = "max", placeholder = "value")}
)
public class ThresholdRuleDetectionStage extends StaticAnomalyDetectionStage {
  private final String COL_TOO_HIGH = "tooHigh";
  private final String COL_TOO_LOW = "tooLow";
  private final String COL_ANOMALY = "anomaly";

  private double min;
  private double max;
  private MetricSlice slice;
  private Long configId;
  private Long endTime;

  @Override
  public InputDataSpec getInputDataSpec() {
    return new InputDataSpec()
        .withTimeseriesSlices(Collections.singletonList(this.slice));
  }

  @Override
  List<MergedAnomalyResultDTO> runDetection(InputData data) {
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

    return this.makeAnomalies(this.slice, df, COL_ANOMALY, this.configId, this.endTime);
  }



  @Override
  public void init(Map<String, Object> specs, Long configId, long startTime, long endTime) {
    this.min = MapUtils.getDoubleValue(specs, "min", Double.NaN);
    this.max = MapUtils.getDoubleValue(specs, "max", Double.NaN);
    this.configId = configId;
    this.endTime = endTime;
    String metricUrn = MapUtils.getString(specs, "metricUrn");
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    this.slice = MetricSlice.from(me.getId(), startTime, endTime, me.getFilters());
  }
}
