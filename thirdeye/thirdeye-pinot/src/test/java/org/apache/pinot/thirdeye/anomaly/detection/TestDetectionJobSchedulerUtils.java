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

package org.apache.pinot.thirdeye.anomaly.detection;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionStatusDTO;

public class TestDetectionJobSchedulerUtils {

  DateTimeFormatter minuteDateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmm").withZone(DateTimeZone.UTC);


  @Test
  public void testGetNewEntriesForDetectionSchedulerHourly() throws Exception {

    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.HOURS);
    datasetConfig.setTimeDuration(1);
    DateTimeZone dateTimeZone = DateTimeZone.UTC;

    AnomalyFunctionDTO anomalyFunction = new AnomalyFunctionDTO();

    DateTimeFormatter dateTimeFormatter = DetectionJobSchedulerUtils.
        getDateTimeFormatterForDataset(datasetConfig, dateTimeZone);
    String currentDateTimeString = "201702140336";
    String currentDateTimeStringRounded = "2017021403";
    DateTime currentDateTime = minuteDateTimeFormatter.parseDateTime(currentDateTimeString);
    DateTime currentDateTimeRounded = dateTimeFormatter.parseDateTime(currentDateTimeStringRounded);
    DetectionStatusDTO lastEntryForFunction = null;

    // null last entry
    Map<String, Long> newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry same as current time
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(currentDateTimeStringRounded);
    lastEntryForFunction.setDateToCheckInMS(currentDateTimeRounded.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 0);

    // last entry 1 hour before current time
    String lastEntryDateTimeString = "2017021402";
    DateTime lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry 3 hours before current time
    lastEntryDateTimeString = "2017021400";
    lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 3);
    Assert.assertNotNull(newEntries.get("2017021401"));
    Assert.assertNotNull(newEntries.get("2017021402"));
    Assert.assertNotNull(newEntries.get("2017021403"));
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

  }

  @Test
  public void testGetNewEntriesForDetectionSchedulerMinuteLevel() throws Exception {

    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.MINUTES);
    datasetConfig.setTimeDuration(5);
    DateTimeZone dateTimeZone = DateTimeZone.UTC;

    AnomalyFunctionDTO anomalyFunction = new AnomalyFunctionDTO();
    anomalyFunction.setFrequency(new TimeGranularity(15, TimeUnit.MINUTES));

    DateTimeFormatter dateTimeFormatter = DetectionJobSchedulerUtils.
        getDateTimeFormatterForDataset(datasetConfig, dateTimeZone);

    String currentDateTimeString = "201702140336";
    String currentDateTimeStringRounded = "201702140330";
    DateTime currentDateTime = minuteDateTimeFormatter.parseDateTime(currentDateTimeString);
    DateTime currentDateTimeRounded = dateTimeFormatter.parseDateTime(currentDateTimeStringRounded);

    DetectionStatusDTO lastEntryForFunction = null;

    // null last entry
    Map<String, Long> newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry same as current time
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(currentDateTimeStringRounded);
    lastEntryForFunction.setDateToCheckInMS(currentDateTimeRounded.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 0);

    // last entry 15 MINUTES before current time
    String lastEntryDateTimeString = "201702140315";
    DateTime lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry 45 MINUTES  before current time
    lastEntryDateTimeString = "201702140245";
    lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 3);
    Assert.assertNotNull(newEntries.get("201702140300"));
    Assert.assertNotNull(newEntries.get("201702140315"));
    Assert.assertNotNull(newEntries.get("201702140330"));
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

  }

  @Test
  public void testGetNewEntriesForDetectionSchedulerDaily() throws Exception {

    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.DAYS);
    datasetConfig.setTimeDuration(1);
    DateTimeZone dateTimeZone = DateTimeZone.UTC;

    AnomalyFunctionDTO anomalyFunction = new AnomalyFunctionDTO();

    DateTimeFormatter dateTimeFormatter = DetectionJobSchedulerUtils.
        getDateTimeFormatterForDataset(datasetConfig, dateTimeZone);
    String currentDateTimeString = "201702140337";
    String currentDateTimeStringRounded = "20170214";
    DateTime currentDateTime = minuteDateTimeFormatter.parseDateTime(currentDateTimeString);
    DateTime currentDateTimeRounded = dateTimeFormatter.parseDateTime(currentDateTimeStringRounded);
    DetectionStatusDTO lastEntryForFunction = null;

    // null last entry
    Map<String, Long> newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry same as current time
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(currentDateTimeStringRounded);
    lastEntryForFunction.setDateToCheckInMS(currentDateTimeRounded.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 0);

    // last entry 1 day before current time
    String lastEntryDateTimeString = "20170213";
    DateTime lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry 3 days before current time
    lastEntryDateTimeString = "20170211";
    lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 3);
    Assert.assertNotNull(newEntries.get("20170212"));
    Assert.assertNotNull(newEntries.get("20170213"));
    Assert.assertNotNull(newEntries.get("20170214"));
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));
  }

  @Test
  public void testGetNewEntriesForDSTBegins() throws Exception {

    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.DAYS);
    datasetConfig.setTimeDuration(1);
    DateTimeZone dateTimeZone = DateTimeZone.UTC;

    AnomalyFunctionDTO anomalyFunction = new AnomalyFunctionDTO();

    // DST started on 2017031203
    DateTimeFormatter dateTimeFormatter = DetectionJobSchedulerUtils.
        getDateTimeFormatterForDataset(datasetConfig, dateTimeZone);
    String currentDateTimeString = "201703120437";
    String currentDateTimeStringRounded = "20170312";
    DateTime currentDateTime = minuteDateTimeFormatter.parseDateTime(currentDateTimeString);
    DateTime currentDateTimeRounded = dateTimeFormatter.parseDateTime(currentDateTimeStringRounded);
    DetectionStatusDTO lastEntryForFunction = null;

    // null last entry
    Map<String, Long> newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry same as current time
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(currentDateTimeStringRounded);
    lastEntryForFunction.setDateToCheckInMS(currentDateTimeRounded.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 0);

    // last entry 1 day before current time, before DST
    String lastEntryDateTimeString = "20170311";
    DateTime lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry 3 days before current time
    lastEntryDateTimeString = "20170309";
    lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 3);
    Assert.assertNotNull(newEntries.get("20170310"));
    Assert.assertNotNull(newEntries.get("20170311"));
    Assert.assertNotNull(newEntries.get("20170312"));
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));
  }

  @Test
  public void testGetNewEntriesForDSTEnds() throws Exception {

    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.DAYS);
    datasetConfig.setTimeDuration(1);
    DateTimeZone dateTimeZone = DateTimeZone.UTC;

    AnomalyFunctionDTO anomalyFunction = new AnomalyFunctionDTO();

    // DST ended on 2016110602
    DateTimeFormatter dateTimeFormatter = DetectionJobSchedulerUtils.
        getDateTimeFormatterForDataset(datasetConfig, dateTimeZone);
    String currentDateTimeString = "201611060437";
    String currentDateTimeStringRounded = "20161106";
    DateTime currentDateTime = minuteDateTimeFormatter.parseDateTime(currentDateTimeString);
    DateTime currentDateTimeRounded = dateTimeFormatter.parseDateTime(currentDateTimeStringRounded);
    DetectionStatusDTO lastEntryForFunction = null;

    // null last entry
    Map<String, Long> newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry same as current time
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(currentDateTimeStringRounded);
    lastEntryForFunction.setDateToCheckInMS(currentDateTimeRounded.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 0);

    // last entry 1 day before current time, before DST ended
    String lastEntryDateTimeString = "20161105";
    DateTime lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));

    // last entry 3 days before current time
    lastEntryDateTimeString = "20161103";
    lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, anomalyFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 3);
    Assert.assertNotNull(newEntries.get("20161104"));
    Assert.assertNotNull(newEntries.get("20161105"));
    Assert.assertNotNull(newEntries.get("20161106"));
    Assert.assertEquals(newEntries.get(currentDateTimeStringRounded), new Long(currentDateTimeRounded.getMillis()));
  }


}
