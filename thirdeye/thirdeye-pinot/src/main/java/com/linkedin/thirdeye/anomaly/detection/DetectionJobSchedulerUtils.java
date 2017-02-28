package com.linkedin.thirdeye.anomaly.detection;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessTaskUtils;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class DetectionJobSchedulerUtils {

  private static final String DAY_FORMAT = "yyyyMMdd";
  private static final String HOUR_FORMAT = "yyyyMMddHH";

  /**
   * Get date time formatter according to granularity of dataset
   * This is to store the date in the db, in the correct SDF
   * @param timeSpec
   * @return
   */
  public static DateTimeFormatter getDateTimeFormatterForDataset(
      DatasetConfigDTO datasetConfig, DateTimeZone dateTimeZone) {
    String pattern = null;
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeUnit unit = timeSpec.getDataGranularity().getUnit();
    switch (unit) {
      case DAYS:
        pattern = DAY_FORMAT;
        break;
      case HOURS:
      default:
        pattern = HOUR_FORMAT;
        break;
    }
    DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(pattern).withZone(dateTimeZone);
    return dateTimeFormatter;
  }

  /**
   * round this time to earlier boundary, depending on granularity of dataset
   * e.g. 12:15pm on HOURLY dataset should be treated as 12pm
   * and 12:50pm on any MINUTE level dataset should be treated as 12:30pm
   * @param timeSpec
   * @param dataCompletenessStartTime
   * @param dateTimeZone
   * @return
   */
  public static long getAdjustedTimeForDataset(DatasetConfigDTO datasetConfig, DateTime dateTime) {
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeUnit unit = timeSpec.getDataGranularity().getUnit();
    switch (unit) {
      case DAYS:
        dateTime = dateTime.withTimeAtStartOfDay();
        break;
      case HOURS:
      default:
        dateTime = dateTime.withTime(dateTime.getHourOfDay(), 0, 0, 0);
        break;
    }
    return dateTime.getMillis();
  }

  /**
   * get bucket size in millis, according to data granularity of dataset
   * Bucket size are 1 HOUR for hourly, 1 DAY for daily
   * @param timeSpec
   * @return
   */
  public static long getBucketSizeInMSForDataset(DatasetConfigDTO datasetConfig) {
    long bucketMillis = 0;
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeUnit unit = timeSpec.getDataGranularity().getUnit();
    switch (unit) {
      case DAYS:
        bucketMillis = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        break;
      case HOURS:
      default:
        bucketMillis = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
        break;
    }
    return bucketMillis;
  }

  /**
   *
   */
  public static Map<String, Long> getNewEntries(DateTime currentDateTime, DetectionStatusDTO lastEntryForFunction,
      DatasetConfigDTO datasetConfig, DateTimeZone dateTimeZone) {

    Map<String, Long> newEntries = new HashMap<>();

    // get current hour/day, depending on granularity of dataset,
    DateTimeFormatter dateTimeFormatterForDataset = DetectionJobSchedulerUtils.
        getDateTimeFormatterForDataset(datasetConfig, dateTimeZone);

    // if first ever entry, create it with current time
    if (lastEntryForFunction == null) {
      String currentDateString = dateTimeFormatterForDataset.print(currentDateTime);
      newEntries.put(currentDateString, dateTimeFormatterForDataset.parseMillis(currentDateString));
    } else { // else create all entries from last entry onwards to current time
      long currentMillis = DetectionJobSchedulerUtils.getAdjustedTimeForDataset(datasetConfig, currentDateTime);
      long lastMillis = lastEntryForFunction.getDateToCheckInMS();
      while (lastMillis < currentMillis) {
        lastMillis = lastMillis + DetectionJobSchedulerUtils.getBucketSizeInMSForDataset(datasetConfig);
        newEntries.put(dateTimeFormatterForDataset.print(lastMillis), lastMillis);
      }
    }
    return newEntries;
  }


  public static String createJobName(AnomalyFunctionDTO anomalyFunction, long startTime, long endTime) {

    return String.format("%s-%s-%s-%s", anomalyFunction.getId(), anomalyFunction.getFunctionName(), startTime, endTime);
  }

  public static long getExpectedCompleteBuckets(DatasetConfigDTO datasetConfig, long startTime, long endTime) {
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    long bucketSize = DataCompletenessTaskUtils.getBucketSizeInMSForDataset(timeSpec);
    long numBuckets = (endTime - startTime)/bucketSize;
    return numBuckets;
  }

}
