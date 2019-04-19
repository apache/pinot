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

import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultInputDataFetcher;
import org.apache.pinot.thirdeye.detection.DetectionTestUtils;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.spec.AbsoluteChangeRuleAnomalyFilterSpec;
import org.apache.pinot.thirdeye.detection.spec.PercentageChangeRuleAnomalyFilterSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyFilter;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregate;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class AbsoluteChangeRuleAnomalyFilterTest {
  private static final String METRIC_URN = "thirdeye:metric:123";

  private DataProvider testDataProvider;
  private Baseline baseline;

  @BeforeMethod
  public void beforeMethod() {
    this.baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEDIAN, 1, 1, DateTimeZone.forID("UTC"));

    MetricSlice slice1 = MetricSlice.from(123L, 0, 2);
    MetricSlice baselineSlice1 = this.baseline.scatter(slice1).get(0);
    MetricSlice slice2 = MetricSlice.from(123L, 4, 6);
    MetricSlice baselineSlice2 = this.baseline.scatter(slice2).get(0);

    Map<MetricSlice, DataFrame> aggregates = new HashMap<>();
    aggregates.put(slice1, new DataFrame().addSeries(COL_VALUE, 150).addSeries(COL_TIME, slice1.getStart()).setIndex(COL_TIME));
    aggregates.put(baselineSlice1, new DataFrame().addSeries(COL_VALUE, 200).addSeries(COL_TIME, baselineSlice1.getStart()).setIndex(COL_TIME));
    aggregates.put(slice2, new DataFrame().addSeries(COL_VALUE, 500).addSeries(COL_TIME, slice2.getStart()).setIndex(COL_TIME));
    aggregates.put(baselineSlice2, new DataFrame().addSeries(COL_VALUE, 1000).addSeries(COL_TIME, baselineSlice2.getStart()).setIndex(COL_TIME));

    this.testDataProvider = new MockDataProvider().setAggregates(aggregates);
  }

  @Test
  public void testAbsoluteChangeFilter(){
    AbsoluteChangeRuleAnomalyFilterSpec spec = new AbsoluteChangeRuleAnomalyFilterSpec();
    spec.setOffset("median1w");
    spec.setThreshold(100);
    spec.setPattern("up_or_down");
    AnomalyFilter filter = new AbsoluteChangeRuleAnomalyFilter();
    filter.init(spec, new DefaultInputDataFetcher(this.testDataProvider, 125L));
    List<Boolean> results =
        Arrays.asList(makeAnomaly(0, 2), makeAnomaly(4, 6)).stream().map(anomaly -> filter.isQualified(anomaly)).collect(
            Collectors.toList());
    Assert.assertEquals(results, Arrays.asList(false, true));
  }


  private static MergedAnomalyResultDTO makeAnomaly(long start, long end) {
    Map<String, String> dimensions = new HashMap<>();
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(125L, start, end, dimensions);
    anomaly.setMetricUrn(METRIC_URN);
    return anomaly;
  }
}
