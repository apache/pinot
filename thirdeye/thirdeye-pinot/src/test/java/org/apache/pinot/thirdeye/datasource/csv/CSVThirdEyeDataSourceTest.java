/**
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

package org.apache.pinot.thirdeye.datasource.csv;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CSVThirdEyeDataSourceTest {
  ThirdEyeDataSource dataSource;

  @BeforeMethod
  public void beforeMethod() {

    Map<String, DataFrame> sources = new HashMap<>();
    sources.put("source", new DataFrame()
        .addSeries("timestamp", 0, 1, 3600000, 7200000, 10800000, 0, 3600000, 7200000, 10800000)
        .addSeries("views", 1000, 88, 1001, 1002, 1003, 2000, 2001, 2002, 2003)
        .addSeries("country", "us", "us", "us", "us", "us", "cn", "cn", "cn", "cn")
        .addSeries("browser", "chrome", "chrome", "chrome","chrome","chrome","chrome","chrome","chrome","safari"));
    sources.put("other", new DataFrame());
    Map<Long, String> metricNameMap = new HashMap<>();
    metricNameMap.put(1L, "views");

    dataSource = CSVThirdEyeDataSource.fromDataFrame(sources, metricNameMap);
  }

  @Test
  public void testFromDataFrame() throws Exception {
    Map<String, DataFrame> sources = new HashMap<>();
    sources.put("a", new DataFrame());
    sources.put("b", new DataFrame());
    sources.put("c", new DataFrame());

    CSVThirdEyeDataSource dataSource = CSVThirdEyeDataSource.fromDataFrame(sources, Collections.<Long, String>emptyMap());
    Assert.assertEquals(new HashSet<>(dataSource.getDatasets()), sources.keySet());
    Assert.assertNotSame(new HashSet<>(dataSource.getDatasets()), sources.keySet());
  }

  @Test
  public void testFromUrl() throws Exception {
    URL business = this.getClass().getResource("business.csv");

    Map<String, URL> sources = new HashMap<>();
    sources.put("business", business);

    CSVThirdEyeDataSource dataSource = CSVThirdEyeDataSource.fromUrl(sources, Collections.<Long, String>emptyMap());

    Map<String, List<String>> dimensions = new HashMap<>();
    dimensions.put("country", Arrays.asList("us", "cn"));
    dimensions.put("browser", Arrays.asList("chrome", "firefox", "safari"));

    Assert.assertEquals(dataSource.getDatasets(), Collections.singletonList("business"));
    Assert.assertEquals(dataSource.getMaxDataTime("business"), 7200000);
    Assert.assertEquals(new HashSet<>(dataSource.getDimensionFilters("business").get("country")), new HashSet<>(dimensions.get("country")));
    Assert.assertEquals(new HashSet<>(dataSource.getDimensionFilters("business").get("browser")), new HashSet<>(dimensions.get("browser")));
  }

  @Test
  public void testGetName() {
    Assert.assertEquals(dataSource.getName(), CSVThirdEyeDataSource.class.getSimpleName());
  }

  @Test
  public void testGetDatasets() throws Exception{
    Assert.assertEquals(new HashSet<>(dataSource.getDatasets()), new HashSet<>(Arrays.asList("source", "other")));
  }

  @Test
  public void testGetMaxDataTime() throws Exception{
    Assert.assertEquals(dataSource.getMaxDataTime("source"), 10800000);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetMaxDataTimeFail() throws Exception {
    dataSource.getMaxDataTime("invalid");
  }

  @Test
  public void testGetDimensionFilters() throws Exception{
    Map<String, List<String>> expectedMap = new HashMap<>();
    expectedMap.put("country", Arrays.asList("cn", "us"));
    expectedMap.put("browser", Arrays.asList("chrome", "safari"));
    Map<String, List<String>> actualMap = dataSource.getDimensionFilters("source");
    Assert.assertEquals(new HashSet<>(actualMap.get("browser")),
        new HashSet<>(expectedMap.get("browser")));
    Assert.assertEquals(new HashSet<>(actualMap.get("country")),
        new HashSet<>(expectedMap.get("country")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetDimensionFiltersFail() throws Exception {
    dataSource.getDimensionFilters("invalid");
  }

  @Test
  public void testExecuteAggregation() throws Exception {
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .build("ref");
    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 12100d)
            .addSeries("timestamp", -1L)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteTimeSeries() throws Exception {
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setGroupByTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS))
        .build("ref");
    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("timestamp", 0, 3600000, 7200000, 10800000)
            .addSeries("SUM_views", 3088, 3002, 3004, 3006)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteSelectTimeSeries() throws Exception {
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setGroupByTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS))
        .setStartTimeInclusive(2000000)
        .setEndTimeExclusive(10800000)
        .build("ref");
    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("timestamp", 3600000, 7200000)
            .addSeries("SUM_views", 3002, 3004)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteSelectAggregation() throws Exception {
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setStartTimeInclusive(1000000)
        .setEndTimeExclusive(11000000)
        .build("ref");
    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 9012.0d)
            .addSeries("timestamp", -1L)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteFilterAggregation() throws Exception {

    Multimap<String, String> filter = ArrayListMultimap.create();
    filter.put("country", "cn");
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setFilterSet(filter)
        .setStartTimeInclusive(3600000)
        .setEndTimeExclusive(11000000)
        .build("ref");

    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 6006d)
            .addSeries("timestamp", -1L)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteMultiFilterAggregation() throws Exception {
    Multimap<String, String> filter = ArrayListMultimap.create();
    filter.put("country", "us");
    filter.put("country", "cn");
    filter.put("browser", "chrome");
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setFilterSet(filter)
        .setStartTimeInclusive(3600000)
        .setEndTimeExclusive(11000000)
        .build("ref");

    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 7009d)
            .addSeries("timestamp", -1L)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteMultiFilterExclusionAggregation() throws Exception {
    Multimap<String, String> filter = ArrayListMultimap.create();
    filter.put("country", "us");
    filter.put("country", "!cn");
    filter.put("browser", "!safari");
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setFilterSet(filter)
        .setStartTimeInclusive(3600000)
        .setEndTimeExclusive(11000000)
        .build("ref");

    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 3006d)
            .addSeries("timestamp", -1L)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteGroupByColumns() throws Exception {

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .addGroupBy(Arrays.asList("country", "browser"))
        .build("ref");

    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("country", "cn", "us", "cn")
            .addSeries("browser", "chrome", "chrome", "safari")
            .addSeries("SUM_views", 6003, 4094, 2003)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteGroupByOneColumn() throws Exception {

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .addGroupBy(Collections.singletonList("country"))
        .build("ref");

    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("country", "cn", "us")
            .addSeries("SUM_views", 8006, 4094)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteGroupByColumnsLimit() throws Exception {

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .addGroupBy(Arrays.asList("country", "browser"))
        .setLimit(2)
        .build("ref");

    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("country", "cn", "us")
            .addSeries("browser", "chrome", "chrome")
            .addSeries("SUM_views", 6003, 4094)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

  @Test
  public void testExecuteGroupByTimeGranularityAndColumns() throws Exception {

    Multimap<String, String> filter = ArrayListMultimap.create();
    filter.put("country", "cn");
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setGroupByTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS))
        .addGroupBy(Arrays.asList("country", "browser"))
        .build("ref");

    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("timestamp", 0, 3600000, 7200000, 10800000, 0, 3600000, 7200000, 10800000)
            .addSeries("country", "us", "us","us","us", "cn", "cn","cn","cn")
            .addSeries("browser", "chrome", "chrome", "chrome", "chrome", "chrome", "chrome", "chrome", "safari")
            .addSeries("SUM_views", 1088, 1001, 1002, 1003, 2000, 2001, 2002, 2003)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);
  }

}
