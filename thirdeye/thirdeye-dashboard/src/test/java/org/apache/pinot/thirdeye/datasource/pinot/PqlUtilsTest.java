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

package org.apache.pinot.thirdeye.datasource.pinot;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PqlUtilsTest {
  private static final String COLLECTION = "collection";
  private static final MetricDataset METRIC = new MetricDataset("metric", COLLECTION);

  private DAOTestBase base;
  private Long metricId;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    this.base = DAOTestBase.getInstance();

    LoadingCache<String, DatasetConfigDTO> mockDatasetConfigCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockDatasetConfigCache.get(COLLECTION)).thenReturn(new DatasetConfigDTO());

    LoadingCache<MetricDataset, MetricConfigDTO> mockMetricConfigCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockMetricConfigCache.get(METRIC)).thenReturn(new MetricConfigDTO());

    ThirdEyeCacheRegistry.getInstance().registerDatasetConfigCache(mockDatasetConfigCache);
    ThirdEyeCacheRegistry.getInstance().registerMetricConfigCache(mockMetricConfigCache);

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setDataset(COLLECTION);
    metricConfigDTO.setName(METRIC.getMetricName());
    metricConfigDTO.setAlias(METRIC.getDataset() + "::" + METRIC.getMetricName());

    this.metricId = DAORegistry.getInstance().getMetricConfigDAO().save(metricConfigDTO);
  }

  @AfterMethod
  public void afterMethod() {
    try { this.base.cleanup(); } catch (Exception ignore) {}
  }

  @Test(dataProvider = "betweenClauseArgs")
  public void getBetweenClause(DateTime start, DateTime end, TimeSpec timeSpec, String expected) throws ExecutionException {
    String betweenClause = SqlUtils.getBetweenClause(start, end, timeSpec, "collection");
    Assert.assertEquals(betweenClause, expected);
  }

  @DataProvider(name = "betweenClauseArgs")
  public Object[][] betweenClauseArgs() {
    return new Object[][] {
        // equal start+end, no format
        getBetweenClauseTestArgs("2016-01-01T00:00:00.000+00:00", "2016-01-01T00:00:00.000+00:00",
            "timeColumn", 2, TimeUnit.HOURS, null, " timeColumn = 201612"),
        // "" with date range
        getBetweenClauseTestArgs("2016-01-01T00:00:00.000+00:00", "2016-01-02T00:00:00.000+00:00",
            "timeColumn", 2, TimeUnit.HOURS, null, " timeColumn >= 201612 AND timeColumn < 201624"),
        // equal start+end, with format
        getBetweenClauseTestArgs("2016-01-01T08:00:00.000+00:00", "2016-01-01T08:00:00.000+00:00",
            "timeColumn", 1, TimeUnit.DAYS, "yyyyMMdd", " timeColumn = 20160101"),
        // "" with date range
        getBetweenClauseTestArgs("2016-01-01T08:00:00.000+00:00", "2016-01-02T08:00:00.000+00:00",
            "timeColumn", 1, TimeUnit.DAYS, "yyyyMMdd",
            " timeColumn >= 20160101 AND timeColumn < 20160102"),
        // Incorrectly aligned date ranges, no format
        getBetweenClauseTestArgs("2016-01-01T01:00:00.000+00:00", "2016-01-01T23:00:00.000+00:00",
            "timeColumn", 2, TimeUnit.HOURS, null, " timeColumn >= 201613 AND timeColumn < 201624"),
        // Incorrectly aligned date ranges, with format
        getBetweenClauseTestArgs("2016-01-01T01:00:00.000+00:00", "2016-01-01T23:00:00.000+00:00",
            "timeColumn", 2, TimeUnit.HOURS, "yyyyMMddHH",
            " timeColumn >= 2016010101 AND timeColumn < 2016010123")
    };
  }

  private Object[] getBetweenClauseTestArgs(String startISO, String endISO, String timeColumn,
      int timeGranularitySize, TimeUnit timeGranularityUnit, String timeSpecFormat,
      String expected) {
    return new Object[] {
        new DateTime(startISO, DateTimeZone.UTC), new DateTime(endISO, DateTimeZone.UTC),
        new TimeSpec(timeColumn, new TimeGranularity(timeGranularitySize, timeGranularityUnit),
            timeSpecFormat),
        expected
    };
  }

  @Test
  public void testGetDimensionWhereClause() {
    Multimap<String, String> dimensions = ArrayListMultimap.create();
    dimensions.put("key", "value");
    dimensions.put("key", "!value");
    dimensions.put("key", "<value");
    dimensions.put("key", "<=value");
    dimensions.put("key", ">value");
    dimensions.put("key", ">=value");
    dimensions.put("key1", "value11");
    dimensions.put("key1", "value12");
    dimensions.put("key2", "!value21");
    dimensions.put("key2", "!value22");
    dimensions.put("key3", "<value3");
    dimensions.put("key4", "<=value4");
    dimensions.put("key5", ">value5");
    dimensions.put("key6", ">=value6");
    dimensions.put("key7", "value71\'");
    dimensions.put("key7", "value72\"");

    String output = SqlUtils.getDimensionWhereClause(dimensions);

    Assert.assertEquals(output, ""
        + "key < \"value\" AND "
        + "key <= \"value\" AND "
        + "key > \"value\" AND "
        + "key >= \"value\" AND "
        + "key IN (\"value\") AND "
        + "key NOT IN (\"value\") AND "
        + "key1 IN (\"value11\", \"value12\") AND "
        + "key2 NOT IN (\"value21\", \"value22\") AND "
        + "key3 < \"value3\" AND "
        + "key4 <= \"value4\" AND "
        + "key5 > \"value5\" AND "
        + "key6 >= \"value6\" AND "
        + "key7 IN (\"value71\'\", \'value72\"\')");
  }

  @Test
  public  void testQuote() {
    Assert.assertEquals(SqlUtils.quote("123"), "123");
    Assert.assertEquals(SqlUtils.quote("abc"), "\"abc\"");
    Assert.assertEquals(SqlUtils.quote("123\'"), "\"123\'\"");
    Assert.assertEquals(SqlUtils.quote("abc\""), "\'abc\"\'");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public  void testQuoteFail() {
    SqlUtils.quote("123\"\'");
  }

  @Test
  public void testLimit() throws Exception {
    MetricFunction metricFunction = new MetricFunction(MetricAggFunction.AVG, METRIC.getMetricName(), this.metricId, COLLECTION, null, null);

    TimeSpec timeSpec = new TimeSpec("Date", TimeGranularity.fromString("1_SECONDS"), TimeSpec.SINCE_EPOCH_FORMAT);

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .setMetricFunctions(Collections.singletonList(metricFunction))
        .setStartTimeInclusive(1000)
        .setEndTimeExclusive(2000)
        .setGroupBy("dimension")
        .setLimit(12345)
        .build("ref");

    String pql = SqlUtils.getSql(request, metricFunction, ArrayListMultimap.<String, String>create(), timeSpec);

    Assert.assertEquals(pql, "SELECT dimension, AVG(metric) FROM collection WHERE  Date >= 1 AND Date < 2 GROUP BY dimension LIMIT 12345");
  }

  @Test
  public void testLimitDefault() throws Exception {
    MetricFunction metricFunction = new MetricFunction(MetricAggFunction.AVG, METRIC.getMetricName(), this.metricId, COLLECTION, null, null);

    TimeSpec timeSpec = new TimeSpec("Date", TimeGranularity.fromString("1_SECONDS"), TimeSpec.SINCE_EPOCH_FORMAT);

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .setMetricFunctions(Collections.singletonList(metricFunction))
        .setStartTimeInclusive(1000)
        .setEndTimeExclusive(2000)
        .setGroupBy("dimension")
        .build("ref");

    String pql = SqlUtils.getSql(request, metricFunction, ArrayListMultimap.<String, String>create(), timeSpec);

    Assert.assertEquals(pql, "SELECT dimension, AVG(metric) FROM collection WHERE  Date >= 1 AND Date < 2 GROUP BY dimension LIMIT 100000");
  }
}
