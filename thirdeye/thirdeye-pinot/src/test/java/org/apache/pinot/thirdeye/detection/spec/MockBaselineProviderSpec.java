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

package org.apache.pinot.thirdeye.detection.spec;

import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;
import java.util.Map;


public class MockBaselineProviderSpec extends AbstractSpec {
  private Map<MetricSlice, TimeSeries> baselineTimeseries;
  private Map<MetricSlice, Double> baselineAggregates;

  public Map<MetricSlice, TimeSeries> getBaselineTimeseries() {
    return baselineTimeseries;
  }

  public void setBaselineTimeseries(Map<MetricSlice, TimeSeries> baselineTimeseries) {
    this.baselineTimeseries = baselineTimeseries;
  }

  public Map<MetricSlice, Double> getBaselineAggregates() {
    return baselineAggregates;
  }

  public void setBaselineAggregates(Map<MetricSlice, Double> baselineAggregates) {
    this.baselineAggregates = baselineAggregates;
  }
}
