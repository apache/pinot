package com.linkedin.thirdeye.completeness.checker;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.pinot.PinotQuery;
import com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util methods for data completeness
 */
public class DataCompletenessUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessUtils.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

  private static final String DAY_FORMAT = "yyyyMMdd";
  private static final String HOUR_FORMAT = "yyyyMMddHH";
  private static final String MINUTE_FORMAT = "yyyyMMddHHmm";
  /**
   * All MINUTE granularity data will be rounded off to 30 MINUTES by default
   */
  private static final int MINUTES_LEVEL_ROUNDING = 30;


  // HELPER methods for DataCompletenessTaskRunner

  /**
   * round this time to earlier boundary, depending on granularity of dataset
   * e.g. 12:15pm on HOURLY dataset should be treated as 12pm
   * and 12:50pm on any MINUTE level dataset should be treated as 12:30pm
   * @param timeSpec
   * @param dataCompletenessStartTime
   * @param dateTimeZone
   * @return
   */
  public static long getAdjustedTimeForDataset(TimeSpec timeSpec, long dataCompletenessStartTime, DateTimeZone zone) {
    DateTime adjustedDateTime = new DateTime(dataCompletenessStartTime, zone);
    TimeUnit unit = timeSpec.getDataGranularity().getUnit();
    switch (unit) {
      case DAYS:
        adjustedDateTime = adjustedDateTime.withTimeAtStartOfDay();
        break;
      case MINUTES:
        int roundedMinutes = (adjustedDateTime.getMinuteOfHour()/MINUTES_LEVEL_ROUNDING) * MINUTES_LEVEL_ROUNDING;
        adjustedDateTime = adjustedDateTime.withTime(adjustedDateTime.getHourOfDay(), roundedMinutes, 0, 0);
        break;
      case HOURS:
      default:
        adjustedDateTime = adjustedDateTime.withTime(adjustedDateTime.getHourOfDay(), 0, 0, 0);
        break;
    }
    return adjustedDateTime.getMillis();
  }

  /**
   * get bucket size in millis, according to data granularity of dataset
   * Bucket size are 1 HOUR for hourly, 1 DAY for daily and 30 MINUTES for minute level
   * @param timeSpec
   * @return
   */
  public static long getBucketSizeInMSForDataset(TimeSpec timeSpec) {
    long bucketMillis = 0;
    TimeUnit unit = timeSpec.getDataGranularity().getUnit();
    switch (unit) {
      case DAYS:
        bucketMillis = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        break;
      case MINUTES:
        bucketMillis = TimeUnit.MILLISECONDS.convert(MINUTES_LEVEL_ROUNDING, TimeUnit.MINUTES);
        break;
      case HOURS:
      default:
        bucketMillis = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
        break;
    }
    return bucketMillis;
  }


  /**
   * Get date time formatter according to granularity of dataset
   * This is to store the date in the db, in the correct SDF
   * @param timeSpec
   * @return
   */
  public static DateTimeFormatter getDateTimeFormatterForDataset(TimeSpec timeSpec, DateTimeZone zone) {
    String pattern = null;
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
    DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(pattern).withZone(zone);
    return dateTimeFormatter;
  }

  /**
   * Get time values which correspond to time column in the pinot segment, for the given buckets in millis
   * Knowing millis time or sdf is not sufficient for querying to pinot, as pinot might be storing in sinceEpoch format
   * @param timeSpec
   * @param bucketNameToBucketValue
   * @return
   */
  public static ListMultimap<String, Long> getBucketNameToTimeValuesMap(TimeSpec timeSpec,
      Map<String, Long> bucketNameToBucketValue) {
    ListMultimap<String, Long> bucketNameToTimeValues = ArrayListMultimap.create();

    String timeFormat = timeSpec.getFormat();
    if (timeFormat.equals(TimeSpec.SINCE_EPOCH_FORMAT)) {
      TimeUnit unit = timeSpec.getDataGranularity().getUnit();
      int timeDuration = timeSpec.getDataGranularity().getSize();

      for (Entry<String, Long> entry : bucketNameToBucketValue.entrySet()) {
        String bucketName = entry.getKey();
        Long bucketValue = entry.getValue();
        long timeValue = 0;

        switch (unit) {
          case MINUTES:
            for (int i = 0; i < MINUTES_LEVEL_ROUNDING/timeDuration; i++) {
              timeValue = TimeUnit.MINUTES.convert(bucketValue, TimeUnit.MILLISECONDS) / timeDuration;
              bucketNameToTimeValues.put(bucketName, timeValue);
              bucketValue = bucketValue + TimeUnit.MILLISECONDS.convert(timeDuration, TimeUnit.MINUTES);
            }
            break;
          case DAYS:
            timeValue = TimeUnit.DAYS.convert(bucketValue, TimeUnit.MILLISECONDS);
            bucketNameToTimeValues.put(bucketName, timeValue);
            break;
          case HOURS:
          default:
            timeValue = TimeUnit.HOURS.convert(bucketValue, TimeUnit.MILLISECONDS);
            bucketNameToTimeValues.put(bucketName, timeValue);
            break;
        }
      }
    } else {
      for (Entry<String, Long> entry : bucketNameToBucketValue.entrySet()) {
        String bucketName = entry.getKey();
        bucketNameToTimeValues.put(bucketName, Long.valueOf(bucketName));
      }
    }
    return bucketNameToTimeValues;
  }

  /**
   * Get count * of buckets
   * @param dataset
   * @param bucketNameToBucketValueMS
   * @param bucketNameToBucketValue
   * @return
   */
  public static Map<String, Long> getCountsForBucketsOfDataset(String dataset, TimeSpec timeSpec,
      Map<String, Long> bucketNameToBucketValueMS) {

    // get time values according to dataset timeSpec schema (epoch or sdf values in proper granularity)
    // dateToCheckInSDF -> timeValues as present in segments
    // This is a multimap because for nMinutesSinceEpoch, a bucket (30 minutes) can have more than 1 time values in the 30 minutes
    // e.g.: For 5 minutes granularity data, the checker will round to 30 minutes,
    // but count * should be taken from 6 time values in that 30 minutes
    ListMultimap<String, Long> bucketNameToTimeValues = getBucketNameToTimeValuesMap(timeSpec, bucketNameToBucketValueMS);
    LOG.info("Bucket name to time values {}", bucketNameToTimeValues);

    Map<String, Long> bucketNameToCountStarMap = getBucketNameToCountStarMap(dataset, timeSpec, bucketNameToTimeValues);
    return bucketNameToCountStarMap;
  }

  private static Map<String, Long> getBucketNameToCountStarMap(String dataset, TimeSpec timeSpec,
      ListMultimap<String, Long> bucketNameToTimeValues) {

    Map<String, Long> bucketNameToCountStar = new HashMap<>();

    // generate request
    StringBuilder sb = new StringBuilder();
    String delimiter = "";
    for (Long timeValue : bucketNameToTimeValues.values()) {
      sb.append(delimiter);
      delimiter = " OR ";
      sb.append(String.format("%s='%s'", timeSpec.getColumnName(), timeValue));
    }
    long top = bucketNameToTimeValues.values().size();
    String pql = String.format("select count(*) from %s where %s group by %s top %s",
        dataset, sb.toString(), timeSpec.getColumnName(), top);
    Map<Long, Long> timeValueToCount = new HashMap<>();
    try {
      PinotThirdEyeDataSource pinotThirdEyeDataSource = (PinotThirdEyeDataSource) CACHE_REGISTRY.getQueryCache()
          .getDataSource(PinotThirdEyeDataSource.DATA_SOURCE_NAME);
      ThirdEyeResultSetGroup resultSetGroup = pinotThirdEyeDataSource.executePQL(new PinotQuery(pql, dataset));
      if (resultSetGroup == null || resultSetGroup.size() <= 0) {
        return bucketNameToCountStar;
      }
      ThirdEyeResultSet resultSet = resultSetGroup.get(0);
      for (int i = 0; i < resultSet.getRowCount(); i++) {
        Long timeValue = Long.valueOf(resultSet.getGroupKeyColumnValue(i, 0));
        Long count = resultSet.getLong(i, 0);
        timeValueToCount.put(timeValue, count);
      }


    } catch (ExecutionException e) {
      LOG.error("Exception in getting count *. PQL:{}", pql, e);
    }

    // parse response to get counts
    for (String bucketName : bucketNameToTimeValues.keySet()) {
      List<Long> timeValues = bucketNameToTimeValues.get(bucketName);
      Long sumOfCountForBucket = 0L;
      for (Long timeValue : timeValues) {
        long val = 0L;
        if(timeValueToCount.containsKey(timeValue)) {
          val = timeValueToCount.get(timeValue);
        }
        sumOfCountForBucket = sumOfCountForBucket + val;
      }
      bucketNameToCountStar.put(bucketName, sumOfCountForBucket);
    }

    return bucketNameToCountStar;
  }

  public static double getPercentCompleteness(PercentCompletenessFunctionInput input) {
    String algorithmClass = input.getAlgorithmClass();
    List<Long> baselineCounts = input.getBaselineCounts();
    Long currentCount = input.getCurrentCount();

    DataCompletenessAlgorithm dataCompletenessAlgorithm =
        DataCompletenessAlgorithmFactory.getDataCompletenessAlgorithmFromClass(algorithmClass);
    return dataCompletenessAlgorithm.getPercentCompleteness(baselineCounts, currentCount);
  }
}
