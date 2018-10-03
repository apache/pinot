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

package com.linkedin.thirdeye.completeness.checker;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;

public class DataCompletenessUtilsTest {



  @Test
  public void testGetAdjustedStartForDataset() throws Exception {
    DateTimeZone zone = DateTimeZone.forID("America/Los_Angeles");
    DateTime dateTime1 = new DateTime(2017, 01, 12, 15, 46, zone);

    // SDF, DAYS
    long startTime = dateTime1.getMillis();
    String columnName = "Date";
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);
    String timeFormat = "SIMPLE_DATE_FORMAT:yyyyMMdd";
    TimeSpec timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    long adjustedStartTime = DataCompletenessUtils.getAdjustedTimeForDataset(timeSpec, startTime, zone);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 0, 0, zone).getMillis());

    // EPOCH
    timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;
    // HOURS
    zone = DateTimeZone.UTC;
    dateTime1 = new DateTime(2017, 01, 12, 15, 46, zone);
    startTime = dateTime1.getMillis();
    timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    adjustedStartTime = DataCompletenessUtils.getAdjustedTimeForDataset(timeSpec, startTime, zone);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 0, zone).getMillis());

    // DEFAULT
    timeGranularity = new TimeGranularity(1, TimeUnit.MILLISECONDS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    adjustedStartTime = DataCompletenessUtils.getAdjustedTimeForDataset(timeSpec, startTime, zone);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 0, zone).getMillis());

    // MINUTES
    timeGranularity = new TimeGranularity(5, TimeUnit.MINUTES);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    adjustedStartTime = DataCompletenessUtils.getAdjustedTimeForDataset(timeSpec, startTime, zone);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 30, zone).getMillis());

    DateTime dateTime2 = new DateTime(2017, 01, 12, 15, 00, zone);
    DateTime dateTime3 = new DateTime(2017, 01, 12, 15, 03, zone);
    startTime = dateTime2.getMillis();
    adjustedStartTime = DataCompletenessUtils.getAdjustedTimeForDataset(timeSpec, startTime, zone);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 0, zone).getMillis());

    startTime = dateTime3.getMillis();
    adjustedStartTime = DataCompletenessUtils.getAdjustedTimeForDataset(timeSpec, startTime, zone);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 0, zone).getMillis());
  }

  @Test
  public void testGetBucketSizeForDataset() throws Exception {
    String columnName = "Date";
    // DAYS bucket
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);
    String timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;
    TimeSpec timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    long bucketSize = DataCompletenessUtils.getBucketSizeInMSForDataset(timeSpec);
    Assert.assertEquals(bucketSize, 24*60*60_000);

    // HOURS bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketSize = DataCompletenessUtils.getBucketSizeInMSForDataset(timeSpec);
    Assert.assertEquals(bucketSize, 60*60_000);

    // MINUTES returns 30 MINUTES bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.MINUTES);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketSize = DataCompletenessUtils.getBucketSizeInMSForDataset(timeSpec);
    Assert.assertEquals(bucketSize, 30*60_000);

    // DEFAULT bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.MILLISECONDS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketSize = DataCompletenessUtils.getBucketSizeInMSForDataset(timeSpec);
    Assert.assertEquals(bucketSize, 60*60_000);

  }

  @Test
  public void testGetDateTimeFormatterForDataset() {

    DateTimeZone zone = DateTimeZone.UTC;
    long dateTimeInMS = new DateTime(2017, 01, 12, 15, 30, zone).getMillis();

    String columnName = "Date";
    // DAYS bucket
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);
    String timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;
    TimeSpec timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    DateTimeFormatter dateTimeFormatter = DataCompletenessUtils.getDateTimeFormatterForDataset(timeSpec, zone);
    Assert.assertEquals(dateTimeFormatter.print(dateTimeInMS), "20170112");

    zone = DateTimeZone.forID("America/Los_Angeles");
    long dateTimeInMS1 = new DateTime(2017, 01, 12, 05, 30, zone).getMillis();
    // DAYS bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    dateTimeFormatter = DataCompletenessUtils.getDateTimeFormatterForDataset(timeSpec, zone);
    Assert.assertEquals(dateTimeFormatter.print(dateTimeInMS1), "20170112");

    // HOURS bucket
    zone = DateTimeZone.UTC;
    dateTimeInMS = new DateTime(2017, 01, 12, 15, 30, zone).getMillis();
    timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    dateTimeFormatter = DataCompletenessUtils.getDateTimeFormatterForDataset(timeSpec, zone);
    Assert.assertEquals(dateTimeFormatter.print(dateTimeInMS), "2017011215");

    // MINUTES bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.MINUTES);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    dateTimeFormatter = DataCompletenessUtils.getDateTimeFormatterForDataset(timeSpec, zone);
    Assert.assertEquals(dateTimeFormatter.print(dateTimeInMS), "201701121530");

    // DEFAULT bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.MILLISECONDS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    dateTimeFormatter = DataCompletenessUtils.getDateTimeFormatterForDataset(timeSpec, zone);
    Assert.assertEquals(dateTimeFormatter.print(dateTimeInMS), "2017011215");
  }

  @Test
  public void testGetBucketNameToTimeValuesMap() {

    DateTimeZone zone = DateTimeZone.forID("America/Los_Angeles");
    // SDF
    String columnName = "Date";
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);
    String timeFormat = "SIMPLE_DATE_FORMAT:yyyyMMdd";
    TimeSpec timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);

    // DAYS
    Map<String, Long> bucketNameToBucketValue = new HashMap<>();
    bucketNameToBucketValue.put("20170112", new DateTime(2017, 01, 12, 0, 0, zone).getMillis());
    bucketNameToBucketValue.put("20170113", new DateTime(2017, 01, 13, 0, 0, zone).getMillis());
    bucketNameToBucketValue.put("20170114", new DateTime(2017, 01, 14, 0, 0, zone).getMillis());
    Map<String, Long> expectedValues = new HashMap<>();
    expectedValues.put("20170112", 20170112L);
    expectedValues.put("20170113", 20170113L);
    expectedValues.put("20170114", 20170114L);
    ListMultimap<String,Long> bucketNameToTimeValuesMap = DataCompletenessUtils.getBucketNameToTimeValuesMap(timeSpec, bucketNameToBucketValue);
    for (Entry<String, Long> entry : bucketNameToTimeValuesMap.entries()) {
      String bucketName = entry.getKey();
      Assert.assertEquals(entry.getValue(), expectedValues.get(bucketName));
    }

    // EPOCH
    zone = DateTimeZone.UTC;
    timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);

    // HOURS
    timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketNameToBucketValue = new HashMap<>();
    bucketNameToBucketValue.put("2017011200", new DateTime(2017, 01, 12, 0, 0, zone).getMillis());
    bucketNameToBucketValue.put("2017011201", new DateTime(2017, 01, 12, 1, 0, zone).getMillis());
    bucketNameToBucketValue.put("2017011202", new DateTime(2017, 01, 12, 2, 0, zone).getMillis());
    expectedValues = new HashMap<>();
    expectedValues.put("2017011200", 412272L); // hours since epoch values
    expectedValues.put("2017011201", 412273L);
    expectedValues.put("2017011202", 412274L);
    bucketNameToTimeValuesMap = DataCompletenessUtils.getBucketNameToTimeValuesMap(timeSpec, bucketNameToBucketValue);
    for (Entry<String, Long> entry : bucketNameToTimeValuesMap.entries()) {
      String bucketName = entry.getKey();
      Assert.assertEquals(entry.getValue(), expectedValues.get(bucketName));
    }

    // MINUTES
    timeGranularity = new TimeGranularity(10, TimeUnit.MINUTES);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketNameToBucketValue = new HashMap<>();
    bucketNameToBucketValue.put("201701120000", new DateTime(2017, 01, 12, 0, 0, zone).getMillis());
    bucketNameToBucketValue.put("201701120030", new DateTime(2017, 01, 12, 0, 30, zone).getMillis());
    bucketNameToBucketValue.put("201701120100", new DateTime(2017, 01, 12, 1, 00, zone).getMillis());
    bucketNameToBucketValue.put("201701120130", new DateTime(2017, 01, 12, 1, 30, zone).getMillis());
    Map<String, List<Long>> expectedValuesList = new HashMap<>();
    expectedValuesList.put("201701120000", Lists.newArrayList(2473632L, 2473633L, 2473634L)); // 10 minutes since epoch values
    expectedValuesList.put("201701120030", Lists.newArrayList(2473635L, 2473636L, 2473637L));
    expectedValuesList.put("201701120100", Lists.newArrayList(2473638L, 2473639L, 2473640L));
    expectedValuesList.put("201701120130", Lists.newArrayList(2473641L, 2473642L, 2473643L));

    bucketNameToTimeValuesMap = DataCompletenessUtils.getBucketNameToTimeValuesMap(timeSpec, bucketNameToBucketValue);
    for (String bucketName : bucketNameToTimeValuesMap.keySet()) {
      List<Long> timeValues = bucketNameToTimeValuesMap.get(bucketName);
      Collections.sort(timeValues);
      Assert.assertEquals(timeValues, expectedValuesList.get(bucketName));
    }
  }


}
