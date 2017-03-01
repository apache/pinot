package com.linkedin.thirdeye.anomaly.detection;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessTaskUtils;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class DetectionJobSchedulerUtils {

  private static final String DAY_FORMAT = "yyyyMMdd";
  private static final String HOUR_FORMAT = "yyyyMMddHH";
  private static final String MINUTE_FORMAT = "yyyyMMdHHmm";

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
      case MINUTES:
        pattern = MINUTE_FORMAT;
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
   * @param anomalyFunction
   * @param timeSpec
   * @param dataCompletenessStartTime
   * @param dateTimeZone
   * @return
   */
  public static long getBoundaryAlignedTimeForDataset(DatasetConfigDTO datasetConfig, DateTime dateTime,
      AnomalyFunctionDTO anomalyFunction) {
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeUnit dataUnit = timeSpec.getDataGranularity().getUnit();
    TimeGranularity functionFrequency = anomalyFunction.getFrequency();

    // For nMINUTE level datasets, with frequency defined in nMINUTES in the function, (make sure size doesnt exceed 30 minutes, just use 1 HOUR in that case)
    // Calculate time periods according to the function frequency
    if (dataUnit.equals(TimeUnit.MINUTES)) {
      if (functionFrequency.getUnit().equals(TimeUnit.MINUTES) && (functionFrequency.getSize() <=30)) {
        int minuteBucketSize = functionFrequency.getSize();
        int roundedMinutes = (dateTime.getMinuteOfHour()/minuteBucketSize) * minuteBucketSize;
        dateTime = dateTime.withTime(dateTime.getHourOfDay(), roundedMinutes, 0, 0);
      } else {
        dateTime = getBoundaryAlignedTimeForDataset(dateTime, TimeUnit.HOURS);
      }
    } else {
      dateTime = getBoundaryAlignedTimeForDataset(dateTime, dataUnit);
    }

    return dateTime.getMillis();
  }

  private static DateTime getBoundaryAlignedTimeForDataset(DateTime dateTime, TimeUnit unit) {
    switch (unit) {
      case DAYS:
        dateTime = dateTime.withTimeAtStartOfDay();
        break;
      case HOURS:
      default:
        dateTime = dateTime.withTime(dateTime.getHourOfDay(), 0, 0, 0);
        break;
    }
    return dateTime;
  }

  /**
   * get bucket size in millis, according to data granularity of dataset
   * Bucket size are 1 HOUR for hourly, 1 DAY for daily
   * For MINUTE level data, bucket size is calculated based on anomaly function frequency
   * @param timeSpec
   * @return
   */
  public static long getBucketSizeInMSForDataset(DatasetConfigDTO datasetConfig, AnomalyFunctionDTO anomalyFunction) {
    long bucketMillis = 0;
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeUnit dataUnit = timeSpec.getDataGranularity().getUnit();
    TimeGranularity functionFrequency = anomalyFunction.getFrequency();

    // For nMINUTE level datasets, with frequency defined in nMINUTES in the function, (make sure size doesnt exceed 30 minutes, just use 1 HOUR in that case)
    // Calculate time periods according to the function frequency
    if (dataUnit.equals(TimeUnit.MINUTES)) {
      if (functionFrequency.getUnit().equals(TimeUnit.MINUTES) && (functionFrequency.getSize() <=30)) {
        bucketMillis = getBucketSizeInMSForDataset(functionFrequency.getSize(), dataUnit);
      } else {
        bucketMillis = getBucketSizeInMSForDataset(1, TimeUnit.HOURS);
      }
    } else {
      bucketMillis = getBucketSizeInMSForDataset(1, dataUnit);
    }
    return bucketMillis;
  }

  private static long getBucketSizeInMSForDataset(int size, TimeUnit unit) {
    long bucketMillis = TimeUnit.MILLISECONDS.convert(size, unit);
    return bucketMillis;
  }

  /**
   * Create new entries from last entry to current time,
   * according to time granularity of dataset in case of HOURLY/DAILY,
   * and according to time granularity of function frequency in case of MINUTE level data
   * If it is an HOURLY dataset, run detection for every HOUR
   * If it is a DAILY dataset, run detection for every DAY
   * If it is an n MINUTE level dataset, run detection for every bucket, determined by the frequency field in anomaly function
   *
   * @param currentDateTime
   * @param lastEntryForFunction
   * @param anomalyFunction
   * @param datasetConfig
   * @param dateTimeZone
   * @return
   */
  public static Map<String, Long> getNewEntries(DateTime currentDateTime, DetectionStatusDTO lastEntryForFunction,
      AnomalyFunctionDTO anomalyFunction, DatasetConfigDTO datasetConfig, DateTimeZone dateTimeZone) {

    Map<String, Long> newEntries = new LinkedHashMap<>();

    // get current hour/day, depending on granularity of dataset,
    DateTimeFormatter dateTimeFormatterForDataset = DetectionJobSchedulerUtils.
        getDateTimeFormatterForDataset(datasetConfig, dateTimeZone);

    long alignedCurrentMillis =
        DetectionJobSchedulerUtils.getBoundaryAlignedTimeForDataset(datasetConfig, currentDateTime, anomalyFunction);

    // if first ever entry, create it with current time
    if (lastEntryForFunction == null) {
      String currentDateString = dateTimeFormatterForDataset.print(alignedCurrentMillis);
      newEntries.put(currentDateString, dateTimeFormatterForDataset.parseMillis(currentDateString));
    } else { // else create all entries from last entry onwards to current time
      long lastMillis = lastEntryForFunction.getDateToCheckInMS();
      long bucketSize = DetectionJobSchedulerUtils.getBucketSizeInMSForDataset(datasetConfig, anomalyFunction);
      while (lastMillis < alignedCurrentMillis) {
        lastMillis = lastMillis + bucketSize;
        newEntries.put(dateTimeFormatterForDataset.print(lastMillis), lastMillis);
      }
    }
    return newEntries;
  }


  public static String createJobName(AnomalyFunctionDTO anomalyFunction, List<Long> startTimes, List<Long> endTimes) {

    return String.format("%s-%s-%s-%s-%d", anomalyFunction.getId(), anomalyFunction.getFunctionName(),
        startTimes.get(0), endTimes.get(0), startTimes.size());
  }

  public static long getExpectedCompleteBuckets(DatasetConfigDTO datasetConfig, long startTime, long endTime) {
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    long bucketSize = DataCompletenessTaskUtils.getBucketSizeInMSForDataset(timeSpec);
    long numBuckets = (endTime - startTime)/bucketSize;
    return numBuckets;
  }

}
