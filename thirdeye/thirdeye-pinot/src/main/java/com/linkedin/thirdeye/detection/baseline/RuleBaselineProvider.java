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

package com.linkedin.thirdeye.detection.baseline;

import com.linkedin.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.detection.InputData;
import com.linkedin.thirdeye.detection.InputDataSpec;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Baseline provider for rule detection.
 * Supports woXw, meanXw, medianXw, etc.
 *
 * @see BaselineParsingUtils#parseOffset(String, String) for list of all supported offsets
 */
public class RuleBaselineProvider extends StaticBaselineProvider {
  private Baseline baseline;
  private String timezone;
  private String offset;

  @Override
  public void init(Map<String, Object> properties) {
    super.init(properties);
    this.offset = MapUtils.getString(properties, "offset", "current");
    this.timezone = MapUtils.getString(properties, "timezone", "UTC");
    this.baseline = BaselineParsingUtils.parseOffset(this.offset, this.timezone);
  }

  @Override
  InputDataSpec getInputDataSpec(Collection<MetricSlice> slices) {
    List<MetricSlice> timeSeriesSlices = new ArrayList<>();
    for (MetricSlice slice : slices) {
      timeSeriesSlices.addAll(this.baseline.scatter(slice));
    }
    return new InputDataSpec().withTimeseriesSlices(timeSeriesSlices);
  }

  @Override
  public Map<MetricSlice, DataFrame> computeBaselineTimeSeries(Collection<MetricSlice> slices, InputData data) {
    Map<MetricSlice, DataFrame> result = new HashMap<>();
    for (MetricSlice slice : slices) {
      result.put(slice, this.baseline.gather(slice, data.getTimeseries()));
    }
    return result;
  }

  @Override
  InputDataSpec getAggregateInputDataSpec(Collection<MetricSlice> slices) {
    List<MetricSlice> timeSeriesSlices = new ArrayList<>();
    for (MetricSlice slice : slices) {
      timeSeriesSlices.addAll(this.baseline.scatter(slice));
    }
    return new InputDataSpec().withAggregateSlices(timeSeriesSlices);
  }

  @Override
  public Map<MetricSlice, Double> computeBaselineAggregates(Collection<MetricSlice> slices, InputData data) {
    Map<MetricSlice, Double> result = new HashMap<>();
    for (MetricSlice slice : slices) {
      double value;
      try {
        value = data.getAggregates().get(this.baseline.scatter(slice).get(0)).getDouble(COL_VALUE, 0);
      } catch (Exception e) {
        value = Double.NaN;
      }
      result.put(slice, value);
    }
    return result;
  }
}
