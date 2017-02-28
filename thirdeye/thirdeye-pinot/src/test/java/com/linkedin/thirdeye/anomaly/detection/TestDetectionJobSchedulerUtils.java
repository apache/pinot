package com.linkedin.thirdeye.anomaly.detection;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;

public class TestDetectionJobSchedulerUtils {

  @Test
  public void testGetNewEntriesForDetectionSchedulerHourly() throws Exception {

    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.HOURS);
    datasetConfig.setTimeDuration(1);
    DateTimeZone dateTimeZone = DateTimeZone.UTC;

    DateTimeFormatter dateTimeFormatter = DetectionJobSchedulerUtils.
        getDateTimeFormatterForDataset(datasetConfig, dateTimeZone);
    String currentDateTimeString = "2017021403";
    DateTime currentDateTime = dateTimeFormatter.parseDateTime(currentDateTimeString);
    DetectionStatusDTO lastEntryForFunction = null;

    // null last entry
    Map<String, Long> newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeString), new Long(currentDateTime.getMillis()));

    // last entry same as current time
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(currentDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(currentDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 0);

    // last entry 1 hour before current time
    String lastEntryDateTimeString = "2017021402";
    DateTime lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeString), new Long(currentDateTime.getMillis()));

    // last entry 3 hours before current time
    lastEntryDateTimeString = "2017021400";
    lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 3);
    Assert.assertNotNull(newEntries.get("2017021401"));
    Assert.assertNotNull(newEntries.get("2017021402"));
    Assert.assertNotNull(newEntries.get("2017021403"));
    Assert.assertEquals(newEntries.get(currentDateTimeString), new Long(currentDateTime.getMillis()));

  }

  @Test
  public void testGetNewEntriesForDetectionSchedulerDaily() throws Exception {

    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    datasetConfig.setTimeColumn("Date");
    datasetConfig.setTimeUnit(TimeUnit.DAYS);
    datasetConfig.setTimeDuration(1);
    DateTimeZone dateTimeZone = DateTimeZone.UTC;

    DateTimeFormatter dateTimeFormatter = DetectionJobSchedulerUtils.
        getDateTimeFormatterForDataset(datasetConfig, dateTimeZone);
    String currentDateTimeString = "20170214";
    DateTime currentDateTime = dateTimeFormatter.parseDateTime(currentDateTimeString);
    DetectionStatusDTO lastEntryForFunction = null;

    // null last entry
    Map<String, Long> newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeString), new Long(currentDateTime.getMillis()));

    // last entry same as current time
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(currentDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(currentDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 0);

    // last entry 1 day before current time
    String lastEntryDateTimeString = "20170213";
    DateTime lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 1);
    Assert.assertEquals(newEntries.get(currentDateTimeString), new Long(currentDateTime.getMillis()));

    // last entry 3 days before current time
    lastEntryDateTimeString = "20170211";
    lastEntryDateTime = dateTimeFormatter.parseDateTime(lastEntryDateTimeString);
    lastEntryForFunction = new DetectionStatusDTO();
    lastEntryForFunction.setDateToCheckInSDF(lastEntryDateTimeString);
    lastEntryForFunction.setDateToCheckInMS(lastEntryDateTime.getMillis());

    newEntries = DetectionJobSchedulerUtils.
        getNewEntries(currentDateTime, lastEntryForFunction, datasetConfig, dateTimeZone);
    Assert.assertEquals(newEntries.size(), 3);
    Assert.assertNotNull(newEntries.get("20170212"));
    Assert.assertNotNull(newEntries.get("20170213"));
    Assert.assertNotNull(newEntries.get("20170214"));
    Assert.assertEquals(newEntries.get(currentDateTimeString), new Long(currentDateTime.getMillis()));

  }

}
