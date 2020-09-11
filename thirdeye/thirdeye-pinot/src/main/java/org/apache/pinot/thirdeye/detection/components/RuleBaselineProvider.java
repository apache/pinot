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
import java.util.List;
import org.apache.pinot.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.spec.RuleBaselineProviderSpec;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;

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
    return TimeSeries.fromDataFrame(buildBaselines(slice, this.baseline, this.dataFetcher));
  }

  static DataFrame buildBaselines(MetricSlice slice, Baseline baseline, InputDataFetcher dataFetcher) {
    List<MetricSlice> slices = new ArrayList<>(baseline.scatter(slice));
    InputData data = dataFetcher.fetchData(new InputDataSpec().withTimeseriesSlices(slices));
    return baseline.gather(slice, data.getTimeseries());
  }

  @Override
  public void init(RuleBaselineProviderSpec spec, InputDataFetcher dataFetcher) {
    this.offset = spec.getOffset();
    this.timezone = spec.getTimezone();
    this.baseline = BaselineParsingUtils.parseOffset(this.offset, this.timezone);
    this.dataFetcher = dataFetcher;
  }
}
