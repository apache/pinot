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
import org.apache.pinot.thirdeye.detection.spec.SitewideImpactRuleAnomalyFilterSpec;
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

public class SitewideImpactRuleAnomalyFilterTest {
  private static final String METRIC_URN = "thirdeye:metric:123";
  private static final long CONFIG_ID = 125L;

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
    aggregates.put(slice1, new DataFrame().addSeries(DataFrame.COL_VALUE, 150).addSeries(
        DataFrame.COL_TIME, slice1.getStart()).setIndex(DataFrame.COL_TIME));
    aggregates.put(baselineSlice1, new DataFrame().addSeries(DataFrame.COL_VALUE, 200).addSeries(
        DataFrame.COL_TIME, baselineSlice1.getStart()).setIndex(DataFrame.COL_TIME));
    aggregates.put(slice2, new DataFrame().addSeries(DataFrame.COL_VALUE, 500).addSeries(
        DataFrame.COL_TIME, slice2.getStart()).setIndex(DataFrame.COL_TIME));
    aggregates.put(baselineSlice2, new DataFrame().addSeries(DataFrame.COL_VALUE, 1000).addSeries(
        DataFrame.COL_TIME, baselineSlice2.getStart()).setIndex(DataFrame.COL_TIME));

    this.testDataProvider = new MockDataProvider().setAggregates(aggregates);
  }

  @Test
  public void testSiteWideImpactFilter() {
    SitewideImpactRuleAnomalyFilterSpec spec = new SitewideImpactRuleAnomalyFilterSpec();
    spec.setThreshold(0.5);
    spec.setOffset("median3w");
    spec.setPattern("down");
    SitewideImpactRuleAnomalyFilter filter = new SitewideImpactRuleAnomalyFilter();
    filter.init(spec, new DefaultInputDataFetcher(this.testDataProvider, CONFIG_ID));
    List<Boolean> results =
        Arrays.asList(
            DetectionTestUtils.makeAnomaly(0, 2, CONFIG_ID, METRIC_URN, 150), DetectionTestUtils.makeAnomaly(4, 6, CONFIG_ID, METRIC_URN, 500)).stream().map(anomaly -> filter.isQualified(anomaly)).collect(Collectors.toList());
    Assert.assertEquals(results, Arrays.asList(false, true));
  }

  @Test
  public void testSiteWideImpactFilterNoOffset() {
    SitewideImpactRuleAnomalyFilterSpec spec = new SitewideImpactRuleAnomalyFilterSpec();
    spec.setThreshold(0.5);
    spec.setPattern("down");
    SitewideImpactRuleAnomalyFilter filter = new SitewideImpactRuleAnomalyFilter();
    filter.init(spec, new DefaultInputDataFetcher(this.testDataProvider, CONFIG_ID));
    List<MergedAnomalyResultDTO> anomalyResultDTOs = Arrays.asList(
        DetectionTestUtils.makeAnomaly(0, 2, CONFIG_ID, METRIC_URN, 150), DetectionTestUtils.makeAnomaly(4, 6,
        CONFIG_ID, METRIC_URN, 500));
    anomalyResultDTOs.get(0).setAvgCurrentVal(150);
    anomalyResultDTOs.get(0).setAvgBaselineVal(200);
    anomalyResultDTOs.get(1).setAvgCurrentVal(500);
    anomalyResultDTOs.get(1).setAvgBaselineVal(1000);

    List<Boolean> results =
        anomalyResultDTOs.stream().map(anomaly -> filter.isQualified(anomaly)).collect(Collectors.toList());
    Assert.assertEquals(results, Arrays.asList(false, true));
  }
}
