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

package com.linkedin.thirdeye.anomaly.detection;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessUtils;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class DetectionJobSchedulerUtils {

  private static final String DAY_FORMAT = "yyyyMMdd";
  private static final String HOUR_FORMAT = "yyyyMMddHH";
  private static final String MINUTE_FORMAT = "yyyyMMddHHmm";

  /**
   * Get date time formatter according to granularity of dataset
   * This is to store the date in the db, in the correct SDF
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
      case SECONDS:
      case MILLISECONDS:
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
   * any dataset with granularity finer than HOUR, will be rounded as per function frequency (assumption is that this is in MINUTES)
   * so 12.53 on 5 MINUTES dataset, with function frequency 15 MINUTES will be rounded to 12.45
   * @param anomalyFunction
   * @return
   */
  public static long getBoundaryAlignedTimeForDataset(DatasetConfigDTO datasetConfig, DateTime dateTime,
      AnomalyFunctionDTO anomalyFunction) {
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeUnit dataUnit = timeSpec.getDataGranularity().getUnit();
    TimeGranularity functionFrequency = anomalyFunction.getFrequency();

    // For nMINUTE level datasets, with frequency defined in nMINUTES in the function, (make sure size doesnt exceed 30 minutes, just use 1 HOUR in that case)
    // Calculate time periods according to the function frequency
    if (dataUnit.equals(TimeUnit.MINUTES) || dataUnit.equals(TimeUnit.MILLISECONDS) || dataUnit.equals(TimeUnit.SECONDS)) {
      if (functionFrequency.getUnit().equals(TimeUnit.MINUTES) && (functionFrequency.getSize() <=30)) {
        int minuteBucketSize = functionFrequency.getSize();
        int roundedMinutes = (dateTime.getMinuteOfHour()/minuteBucketSize) * minuteBucketSize;
        dateTime = dateTime.withTime(dateTime.getHourOfDay(), roundedMinutes, 0, 0);
      } else {
        dateTime = getBoundaryAlignedTimeForDataset(dateTime, TimeUnit.HOURS); // default to HOURS
      }
    } else {
      dateTime = getBoundaryAlignedTimeForDataset(dateTime, dataUnit);
    }

    return dateTime.getMillis();
  }

  public static DateTime getBoundaryAlignedTimeForDataset(DateTime dateTime, TimeUnit unit) {
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
   * @return
   */
  public static Period getBucketSizePeriodForDataset(DatasetConfigDTO datasetConfig, AnomalyFunctionDTO anomalyFunction) {
    Period bucketSizePeriod = null;
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeUnit dataUnit = timeSpec.getDataGranularity().getUnit();
    TimeGranularity functionFrequency = anomalyFunction.getFrequency();

    // For nMINUTE level datasets, with frequency defined in nMINUTES in the function, (make sure size doesnt exceed 30 minutes, just use 1 HOUR in that case)
    // Calculate time periods according to the function frequency
    if (dataUnit.equals(TimeUnit.MINUTES) || dataUnit.equals(TimeUnit.MILLISECONDS) || dataUnit.equals(TimeUnit.SECONDS)) {
      if (functionFrequency.getUnit().equals(TimeUnit.MINUTES) && (functionFrequency.getSize() <=30)) {
        bucketSizePeriod = new Period(0, 0, 0, 0, 0, functionFrequency.getSize(), 0, 0);
      } else {
        bucketSizePeriod = getBucketSizePeriodForUnit(TimeUnit.HOURS); // default to 1 HOUR
      }
    } else {
      bucketSizePeriod = getBucketSizePeriodForUnit(dataUnit);
    }
    return bucketSizePeriod;
  }

  public static Period getBucketSizePeriodForUnit(TimeUnit unit) {
    Period bucketSizePeriod = null;
    switch (unit) {
      case DAYS:
        bucketSizePeriod = new Period(0, 0, 0, 1, 0, 0, 0, 0); // 1 DAY
        break;
      case HOURS:
      default:
        bucketSizePeriod = new Period(0, 0, 0, 0, 1, 0, 0, 0); //   1 HOUR
        break;
    }
    return bucketSizePeriod;
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
    DateTime alignedDateTime = new DateTime(alignedCurrentMillis, dateTimeZone);

    // if first ever entry, create it with current time
    if (lastEntryForFunction == null) {
      String currentDateString = dateTimeFormatterForDataset.print(alignedDateTime);
      newEntries.put(currentDateString, dateTimeFormatterForDataset.parseMillis(currentDateString));
    } else { // else create all entries from last entry onwards to current time
      DateTime lastDateTime = new DateTime(lastEntryForFunction.getDateToCheckInMS(), dateTimeZone);
      Period bucketSizePeriod = DetectionJobSchedulerUtils.getBucketSizePeriodForDataset(datasetConfig, anomalyFunction);
      while (lastDateTime.isBefore(alignedDateTime)) {
        lastDateTime = lastDateTime.plus(bucketSizePeriod);
        newEntries.put(dateTimeFormatterForDataset.print(lastDateTime), lastDateTime.getMillis());
      }
    }
    return newEntries;
  }


  /**
   * Creates job name for anomaly detection job
   * @param anomalyFunction
   * @param startTimes
   * @param endTimes
   * @return
   */
  public static String createJobName(AnomalyFunctionDTO anomalyFunction, List<Long> startTimes, List<Long> endTimes) {
    return String.format("%s-%s-%s-%s-%d", anomalyFunction.getId(), anomalyFunction.getFunctionName(),
        startTimes.get(0), endTimes.get(0), startTimes.size());
  }

  /**
   * Calculates the number of buckets that a time period can be divided into, depending on the dataset and function frequency
   * @param datasetConfig
   * @param startTime
   * @param endTime
   * @return
   */
  public static long getExpectedCompleteBuckets(DatasetConfigDTO datasetConfig, long startTime, long endTime) {
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    // Get this from DataCompletenessUtils because that determines number of buckets to check
    long bucketSize = DataCompletenessUtils.getBucketSizeInMSForDataset(timeSpec);
    long numBuckets = (endTime - startTime)/bucketSize;
    return numBuckets;
  }

}
