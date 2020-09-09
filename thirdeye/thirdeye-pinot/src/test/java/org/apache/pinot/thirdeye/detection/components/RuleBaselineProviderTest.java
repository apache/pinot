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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.spec.RuleBaselineProviderSpec;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RuleBaselineProviderTest {
  RuleBaselineProvider baselineProvider;
  MockDataProvider dataProvider;
  MetricSlice slice1;
  MetricSlice slice2;

  @BeforeMethod
  public void setUp() {
    baselineProvider = new RuleBaselineProvider();
    slice1 = MetricSlice.from(1L, 1538520728000L, 1538607128000L);
    slice2 = MetricSlice.from(1L, 1538524800000L, 1538611200000L);
    dataProvider = new MockDataProvider();
    MetricSlice slice1Wow = MetricSlice.from(1L, 1537915928000L, 1538002328000L);
    MetricSlice slice2Wow = MetricSlice.from(1L, 1537920000000L, 1538006400000L);
    Map<MetricSlice, DataFrame> aggregates = new HashMap<>();
    aggregates.put(slice1Wow, DataFrame.builder(DataFrame.COL_TIME + ":LONG", DataFrame.COL_VALUE + ":DOUBLE")
        .append(-1, 150)
        .build()
        .setIndex(DataFrame.COL_TIME));
    aggregates.put(slice2Wow, DataFrame.builder(DataFrame.COL_TIME + ":LONG", DataFrame.COL_VALUE + ":DOUBLE")
        .build()
        .setIndex(DataFrame.COL_TIME));
    InputDataFetcher dataFetcher = new DefaultInputDataFetcher(dataProvider, -1);

    baselineProvider.init(new RuleBaselineProviderSpec("UTC", "wo1w"), dataFetcher);

    dataProvider.setTimeseries(Collections.singletonMap(slice1Wow,
        DataFrame.builder(DataFrame.COL_TIME + ":LONG", DataFrame.COL_VALUE + ":DOUBLE")
            .append(1537915928000L, 100)
            .append(1537959128000L, 200)
            .append(1538002328000L, 200)
            .build())).setAggregates(aggregates);
  }

  @Test
  public void testFetchBaselineTimeSeries() {
    DataFrame df = baselineProvider.computePredictedTimeSeries(slice1).getDataFrame();
    Assert.assertEquals(df.getDoubles(DataFrame.COL_VALUE).get(0), 100.0);
    Assert.assertEquals(df.getDoubles(DataFrame.COL_VALUE).get(1), 200.0);
  }


  @Test
  public void testFetchBaselineAggregates() {
    Assert.assertEquals(
        this.baselineProvider.computePredictedAggregates(slice1, DoubleSeries.MEAN), 150.0);
  }

  @Test
  public void testFetchBaselineAggregatesNaN() {
    Assert.assertEquals(
        this.baselineProvider.computePredictedAggregates(slice2, DoubleSeries.MEAN), Double.NaN);
  }
}
