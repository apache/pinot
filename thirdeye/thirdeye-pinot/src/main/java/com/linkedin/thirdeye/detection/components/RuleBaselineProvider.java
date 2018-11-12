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

import com.linkedin.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.annotation.Components;
import com.linkedin.thirdeye.detection.spec.RuleBaselineProviderSpec;
import com.linkedin.thirdeye.detection.spi.components.BaselineProvider;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.detection.spi.model.TimeSeries;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;

@Components(title = "rule baseline",
    type = "RULE_BASELINE"
)
public class RuleBaselineProvider implements BaselineProvider<RuleBaselineProviderSpec> {
  private Baseline baseline;
  private String timezone;
  private String offset;
  private InputDataFetcher dataFetcher;

  @Override
  public TimeSeries computePredictedTimeSeries(MetricSlice slice) {
    InputData data = this.dataFetcher.fetchData(new InputDataSpec().withTimeseriesSlices(this.baseline.scatter(slice)));
    return TimeSeries.fromDataFrame(this.baseline.gather(slice, data.getTimeseries()));
  }

  @Override
  public Double computePredictedAggregates(MetricSlice slice, Series.DoubleFunction aggregateFunction) {
    InputData data = this.dataFetcher.fetchData(new InputDataSpec().withAggregateSlices(this.baseline.scatter(slice)));
    double value;
    try {
      value = data.getAggregates().get(this.baseline.scatter(slice).get(0)).getDouble(COL_VALUE, 0);
    } catch (Exception e) {
      value = Double.NaN;
    }
    return value;
  }


  @Override
  public void init(RuleBaselineProviderSpec spec, InputDataFetcher dataFetcher) {
    this.offset = spec.getOffset();
    this.timezone = spec.getTimezone();
    this.baseline = BaselineParsingUtils.parseOffset(this.offset, this.timezone);
    this.dataFetcher = dataFetcher;
  }
}
