package com.linkedin.thirdeye.completeness.checker;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;

public class DataCompletenessTaskUtilsTest {



  @Test
  public void testGetAdjustedStartForDataset() throws Exception {
    DateTime dateTime1 = new DateTime(2017, 01, 12, 15, 46);
    DateTime dateTime2 = new DateTime(2017, 01, 12, 15, 00);
    DateTime dateTime3 = new DateTime(2017, 01, 12, 15, 03);

    // SDF, DAYS
    long startTime = dateTime1.getMillis();
    String columnName = "Date";
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);
    String timeFormat = "SIMPLE_DATE_FORMAT:yyyyMMdd";
    TimeSpec timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    long adjustedStartTime = DataCompletenessTaskUtils.getAdjustedTimeForDataset(timeSpec, startTime);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 0, 0).getMillis());

    // EPOCH
    timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;
    // HOURS
    timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    adjustedStartTime = DataCompletenessTaskUtils.getAdjustedTimeForDataset(timeSpec, startTime);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 0).getMillis());

    // DEFAULT
    timeGranularity = new TimeGranularity(1, TimeUnit.MILLISECONDS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    adjustedStartTime = DataCompletenessTaskUtils.getAdjustedTimeForDataset(timeSpec, startTime);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 0).getMillis());

    // MINUTES
    timeGranularity = new TimeGranularity(5, TimeUnit.MINUTES);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    adjustedStartTime = DataCompletenessTaskUtils.getAdjustedTimeForDataset(timeSpec, startTime);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 30).getMillis());

    startTime = dateTime2.getMillis();
    adjustedStartTime = DataCompletenessTaskUtils.getAdjustedTimeForDataset(timeSpec, startTime);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 0).getMillis());

    startTime = dateTime3.getMillis();
    adjustedStartTime = DataCompletenessTaskUtils.getAdjustedTimeForDataset(timeSpec, startTime);
    Assert.assertEquals(adjustedStartTime, new DateTime(2017, 01, 12, 15, 0).getMillis());
  }

  @Test
  public void testGetBucketSizeForDataset() throws Exception {
    String columnName = "Date";
    // DAYS bucket
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);
    String timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;
    TimeSpec timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    long bucketSize = DataCompletenessTaskUtils.getBucketSizeInMSForDataset(timeSpec);
    Assert.assertEquals(bucketSize, 24*60*60_000);

    // HOURS bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketSize = DataCompletenessTaskUtils.getBucketSizeInMSForDataset(timeSpec);
    Assert.assertEquals(bucketSize, 60*60_000);

    // MINUTES returns 30 MINUTES bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.MINUTES);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketSize = DataCompletenessTaskUtils.getBucketSizeInMSForDataset(timeSpec);
    Assert.assertEquals(bucketSize, 30*60_000);

    // DEFAULT bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.MILLISECONDS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketSize = DataCompletenessTaskUtils.getBucketSizeInMSForDataset(timeSpec);
    Assert.assertEquals(bucketSize, 60*60_000);

  }

  @Test
  public void testGetDateTimeFormatterForDataset() {

    long dateTimeInMS = new DateTime(2017, 01, 12, 15, 30).getMillis();

    String columnName = "Date";
    // DAYS bucket
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);
    String timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;
    TimeSpec timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    DateTimeFormatter dateTimeFormatter = DataCompletenessTaskUtils.getDateTimeFormatterForDataset(timeSpec);
    Assert.assertEquals(dateTimeFormatter.print(dateTimeInMS), "20170112");

    // HOURS bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    dateTimeFormatter = DataCompletenessTaskUtils.getDateTimeFormatterForDataset(timeSpec);
    Assert.assertEquals(dateTimeFormatter.print(dateTimeInMS), "2017011215");

    // MINUTES bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.MINUTES);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    dateTimeFormatter = DataCompletenessTaskUtils.getDateTimeFormatterForDataset(timeSpec);
    Assert.assertEquals(dateTimeFormatter.print(dateTimeInMS), "201701121530");

    // DEFAULT bucket
    timeGranularity = new TimeGranularity(1, TimeUnit.MILLISECONDS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    dateTimeFormatter = DataCompletenessTaskUtils.getDateTimeFormatterForDataset(timeSpec);
    Assert.assertEquals(dateTimeFormatter.print(dateTimeInMS), "2017011215");
  }

  @Test
  public void testGetBucketNameToTimeValuesMap() {

    // SDF
    String columnName = "Date";
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.DAYS);
    String timeFormat = "SIMPLE_DATE_FORMAT:yyyyMMdd";
    TimeSpec timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);

    // DAYS
    Map<String, Long> bucketNameToBucketValue = new HashMap<>();
    bucketNameToBucketValue.put("20170112", new DateTime(2017, 01, 12, 0, 0).getMillis());
    bucketNameToBucketValue.put("20170113", new DateTime(2017, 01, 13, 0, 0).getMillis());
    bucketNameToBucketValue.put("20170114", new DateTime(2017, 01, 14, 0, 0).getMillis());
    Map<String, Long> expectedValues = new HashMap<>();
    expectedValues.put("20170112", 20170112L);
    expectedValues.put("20170113", 20170113L);
    expectedValues.put("20170114", 20170114L);
    ListMultimap<String,Long> bucketNameToTimeValuesMap = DataCompletenessTaskUtils.getBucketNameToTimeValuesMap(timeSpec, bucketNameToBucketValue);
    for (Entry<String, Long> entry : bucketNameToTimeValuesMap.entries()) {
      String bucketName = entry.getKey();
      Assert.assertEquals(entry.getValue(), expectedValues.get(bucketName));
    }

    // EPOCH
    timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);

    // HOURS
    timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketNameToBucketValue = new HashMap<>();
    bucketNameToBucketValue.put("2017011200", new DateTime(2017, 01, 12, 0, 0).getMillis());
    bucketNameToBucketValue.put("2017011201", new DateTime(2017, 01, 12, 1, 0).getMillis());
    bucketNameToBucketValue.put("2017011202", new DateTime(2017, 01, 12, 2, 0).getMillis());
    expectedValues = new HashMap<>();
    expectedValues.put("2017011200", 412280L); // hours since epoch values
    expectedValues.put("2017011201", 412281L);
    expectedValues.put("2017011202", 412282L);
    bucketNameToTimeValuesMap = DataCompletenessTaskUtils.getBucketNameToTimeValuesMap(timeSpec, bucketNameToBucketValue);
    for (Entry<String, Long> entry : bucketNameToTimeValuesMap.entries()) {
      String bucketName = entry.getKey();
      Assert.assertEquals(entry.getValue(), expectedValues.get(bucketName));
    }

    // MINUTES
    timeGranularity = new TimeGranularity(10, TimeUnit.MINUTES);
    timeSpec = new TimeSpec(columnName, timeGranularity, timeFormat);
    bucketNameToBucketValue = new HashMap<>();
    bucketNameToBucketValue.put("201701120000", new DateTime(2017, 01, 12, 0, 0).getMillis());
    bucketNameToBucketValue.put("201701120030", new DateTime(2017, 01, 12, 0, 30).getMillis());
    bucketNameToBucketValue.put("201701120100", new DateTime(2017, 01, 12, 1, 00).getMillis());
    bucketNameToBucketValue.put("201701120130", new DateTime(2017, 01, 12, 1, 30).getMillis());
    Map<String, List<Long>> expectedValuesList = new HashMap<>();
    expectedValuesList.put("201701120000", Lists.newArrayList(2473680L, 2473681L, 2473682L)); // 10 minutes since epoch values
    expectedValuesList.put("201701120030", Lists.newArrayList(2473683L, 2473684L, 2473685L));
    expectedValuesList.put("201701120100", Lists.newArrayList(2473686L, 2473687L, 2473688L));
    expectedValuesList.put("201701120130", Lists.newArrayList(2473689L, 2473690L, 2473691L));

    bucketNameToTimeValuesMap = DataCompletenessTaskUtils.getBucketNameToTimeValuesMap(timeSpec, bucketNameToBucketValue);
    for (String bucketName : bucketNameToTimeValuesMap.keySet()) {
      List<Long> timeValues = bucketNameToTimeValuesMap.get(bucketName);
      Collections.sort(timeValues);
      Assert.assertEquals(timeValues, expectedValuesList.get(bucketName));
    }
  }


}
