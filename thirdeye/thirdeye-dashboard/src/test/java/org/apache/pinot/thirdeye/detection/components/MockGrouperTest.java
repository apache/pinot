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

import java.util.stream.Stream;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionTestUtils;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.spec.MockGrouperSpec;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregate;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MockGrouperTest {
  private static final String METRIC_URN = "thirdeye:metric:123";

  private DataProvider testDataProvider;
  private Baseline baseline;

  @BeforeMethod
  public void beforeMethod() {
    this.baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEAN, 1, 1, DateTimeZone.forID("UTC"));

    MetricSlice slice1 = MetricSlice.from(123L, 1555570800000L, 1555693200000L);
    MetricSlice baselineSlice1 = this.baseline.scatter(slice1).get(0);
    MetricSlice slice2 = MetricSlice.from(123L, 1554163200000L, 1554249600000L);
    MetricSlice baselineSlice2 = this.baseline.scatter(slice2).get(0);
    MetricSlice slice3 = MetricSlice.from(123L, 1554076800000L, 1554163200000L);
    MetricSlice baselineSlice3 = this.baseline.scatter(slice3).get(0);

    Map<MetricSlice, DataFrame> aggregates = new HashMap<>();
    aggregates.put(slice1, new DataFrame().addSeries(DataFrame.COL_TIME, slice1.getStart()).addSeries(
        DataFrame.COL_VALUE, 150).setIndex(DataFrame.COL_TIME));
    aggregates.put(baselineSlice1, new DataFrame().addSeries(DataFrame.COL_TIME, baselineSlice1.getStart()).addSeries(
        DataFrame.COL_VALUE, 200).setIndex(DataFrame.COL_TIME));
    aggregates.put(slice2, new DataFrame().addSeries(DataFrame.COL_VALUE, 500).addSeries(
        DataFrame.COL_TIME, slice2.getStart()).setIndex(DataFrame.COL_TIME));
    aggregates.put(baselineSlice2, new DataFrame().addSeries(DataFrame.COL_VALUE, 1000).addSeries(
        DataFrame.COL_TIME, baselineSlice2.getStart()).setIndex(DataFrame.COL_TIME));
    aggregates.put(slice3, new DataFrame().addSeries(DataFrame.COL_VALUE, 200).addSeries(
        DataFrame.COL_TIME, slice3.getStart()).setIndex(DataFrame.COL_TIME));
    aggregates.put(baselineSlice3, new DataFrame().addSeries(DataFrame.COL_VALUE, 150).addSeries(
        DataFrame.COL_TIME, baselineSlice3.getStart()).setIndex(DataFrame.COL_TIME));

    this.testDataProvider = new MockDataProvider().setAggregates(aggregates);
  }

  @Test
  public void testGrouperInterface(){
    MockGrouperSpec grouperSpec = new MockGrouperSpec();
    grouperSpec.setMockParam(0.5);

    Map<String, String> dimensions1 = new HashMap<>();
    Map<String, String> dimensions2 = new HashMap<>();
    dimensions1.put("mock_dimension_name", "test_value");

    List<MergedAnomalyResultDTO> candidateAnomalies = Stream.of(
        makeAnomaly(1555570800000L, 1555693200000L, dimensions1),
        makeAnomaly(1554163200000L, 1554249600000L, dimensions2)
    ).collect(Collectors.toList());
    MockGrouper grouper = new MockGrouper();
    List<MergedAnomalyResultDTO> groupedAnomalies = grouper.group(candidateAnomalies);

    // Test if grouper has been triggered successfully
    Assert.assertEquals(groupedAnomalies.size(), 1);
    Assert.assertNotNull(groupedAnomalies.get(0).getProperties());
    Assert.assertEquals(groupedAnomalies.get(0).getProperties().get("TEST_KEY"), "TEST_VALUE");
  }

  private static MergedAnomalyResultDTO makeAnomaly(long start, long end, Map<String, String> dimensions) {
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(125L, start, end, dimensions);
    anomaly.setMetricUrn(METRIC_URN);
    return anomaly;
  }
}
