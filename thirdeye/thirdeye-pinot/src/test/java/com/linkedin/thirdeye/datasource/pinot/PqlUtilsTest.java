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

package com.linkedin.thirdeye.datasource.pinot;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.pinot.PqlUtils;

public class PqlUtilsTest {

  @Test(dataProvider = "betweenClauseArgs")
  public void getBetweenClause(DateTime start, DateTime end, TimeSpec timeFieldSpec,
      String expected) throws ExecutionException {
    String collection = "collection";
    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    LoadingCache<String, DatasetConfigDTO> mockDatasetConfigCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockDatasetConfigCache.get(collection)).thenReturn(datasetConfig);
    ThirdEyeCacheRegistry.getInstance().registerDatasetConfigCache(mockDatasetConfigCache);

    String betweenClause = PqlUtils.getBetweenClause(start, end, timeFieldSpec, "collection");
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

    String output = PqlUtils.getDimensionWhereClause(dimensions);

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
    Assert.assertEquals(PqlUtils.quote("123"), "123");
    Assert.assertEquals(PqlUtils.quote("abc"), "\"abc\"");
    Assert.assertEquals(PqlUtils.quote("123\'"), "\"123\'\"");
    Assert.assertEquals(PqlUtils.quote("abc\""), "\'abc\"\'");
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public  void testQuoteFail() {
    PqlUtils.quote("123\"\'");
  }
}
