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
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.detection.spec.RulebaselineProviderSpec;
import com.linkedin.thirdeye.detection.spi.components.Predictor;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;


public class RuleBasedPredictor implements Predictor<RulebaselineProviderSpec> {
  private Baseline baseline;
  private String timezone;
  private String offset;
  private MetricSlice slice;

  @Override
  public DataFrame computePredictedTimeSeries(InputData data) {
    return this.baseline.gather(slice, data.getTimeseries());
  }

  @Override
  public InputDataSpec getInputDataSpec(MetricSlice slice) {
    this.slice = slice;
    return new InputDataSpec().withTimeseriesSlices(this.baseline.scatter(slice));
  }

  @Override
  public void init(RulebaselineProviderSpec spec) {
    this.offset = spec.getOffset();
    this.timezone = spec.getTimezone();
    this.baseline = BaselineParsingUtils.parseOffset(this.offset, this.timezone);
  }
}
