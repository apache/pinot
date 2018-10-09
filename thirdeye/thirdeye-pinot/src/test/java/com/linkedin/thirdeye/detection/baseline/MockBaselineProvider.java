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

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.detection.DataProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class MockBaselineProvider implements BaselineProvider {
  @Override
  public void init(Map<String, Object> properties) {

  }

  @Override
  public Map<MetricSlice, DataFrame> computeBaselineTimeSeries(Collection<MetricSlice> slices, DataProvider provider) {
    return provider.fetchTimeseries(slices);
  }

  @Override
  public Map<MetricSlice, Double> computeBaselineAggregates(Collection<MetricSlice> slices, DataProvider provider) {
    Map<MetricSlice, DataFrame> data = provider.fetchAggregates(slices, Collections.EMPTY_LIST);
    Map<MetricSlice, Double> result = new HashMap<>();
    for (MetricSlice slice : slices) {
      result.put(slice, data.get(slice).getDouble(COL_VALUE, 0));
    }
    return result;
  }
}
