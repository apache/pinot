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

package org.apache.pinot.thirdeye.detection.components;

import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.spec.MockBaselineProviderSpec;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;


public class MockBaselineProvider implements BaselineProvider<MockBaselineProviderSpec> {
  private MockBaselineProviderSpec mockSpec;

  @Override
  public void init(MockBaselineProviderSpec spec, InputDataFetcher dataFetcher) {
    this.mockSpec = spec;
  }

  @Override
  public TimeSeries computePredictedTimeSeries(MetricSlice slice) {
    return this.mockSpec.getBaselineTimeseries().get(slice);
  }

  @Override
  public Double computePredictedAggregates(MetricSlice slice, Series.DoubleFunction aggregateFunction) {
    return this.mockSpec.getBaselineAggregates().get(slice);
  }
}
