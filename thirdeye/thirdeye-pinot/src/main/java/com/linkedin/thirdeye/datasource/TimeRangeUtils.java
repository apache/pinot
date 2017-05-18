package com.linkedin.thirdeye.datasource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Duration;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import org.joda.time.Hours;

/**
 * Not to be confused with {@link TimeRange}. This class handles splitting time windows into
 * appropriate {@link Range} objects.
 */
public class TimeRangeUtils {

  public static List<Range<DateTime>> computeTimeRanges(TimeGranularity granularity, DateTime start, DateTime end) {
    List<Range<DateTime>> timeranges = new ArrayList<>();
    if (granularity == null) {
      timeranges.add(Range.closedOpen(start, end));
      return timeranges;
    }
    DateTime current = start;
    DateTime newCurrent = null;
    // Duration duration = new Duration(granularity.toMillis());
    while (current.isBefore(end)) {
      // newCurrent = current.plus(granularity.toMillis());
      newCurrent = increment(current, granularity);
      timeranges.add(Range.closedOpen(current, newCurrent));
      current = newCurrent;
    }
    return timeranges;
  }

  public static DateTime increment(DateTime input, TimeGranularity granularity) {
    DateTime output;
    switch (granularity.getUnit()) {
    case DAYS:
      output = input.plusDays(granularity.getSize());
      break;
    case HOURS:
      output = input.plusHours(granularity.getSize());
      break;
    case MILLISECONDS:
      output = input.plusMillis(granularity.getSize());
      break;
    case MINUTES:
      output = input.plusMinutes(granularity.getSize());
      break;
    case SECONDS:
      output = input.plusSeconds(granularity.getSize());
      break;
    default:
      throw new IllegalArgumentException("Timegranularity:" + granularity + " not supported");
    }
    return output;
  }

  /**
   * Given time granularity and start time (with local time zone information), returns the bucket
   * index of the current time (with local time zone information).
   *
   * The reason to use this method to calculate the bucket index is to align the shifted data point
   * due to daylight saving time to the correct bucket index. Note that this method have no effect
   * if the input time use UTC timezone.
   *
   * For instance, considering March 13th 2016, the day DST takes effect. Assume that our daily
   * data whose timestamp is aligned at 0 am at each day, then the data point on March 14th would
   * be actually aligned to 13th's bucket. Because the two data point only has 23 hours difference.
   * Therefore, we cannot calculate the bucket index simply divide the difference between timestamps
   * by millis of 24 hours.
   *
   * We don't need to consider the case of HOURS because the size of a bucket does not change when
   * the time granularity is smaller than DAYS. In DAYS, the bucket size could be 23, 24, or 25
   * hours due to DST. In HOURS or anything smaller, the bucket size does not change. Hence, we
   * simply compute the bucket index using one fixed bucket size (i.e., interval).
   *
   * @param granularity the time granularity of the bucket
   * @param start the start time of the first bucket
   * @param current the current time
   * @return the bucket index of current time
   */
  public static int computeBucketIndex(TimeGranularity granularity, DateTime start, DateTime current) {
    int index = -1;
    switch (granularity.getUnit()) {
    case DAYS:
      Days d = Days.daysBetween(start.toLocalDate(), current.toLocalDate());
      index = d.getDays() / granularity.getSize();
      break;
    default:
      long interval = granularity.toMillis();
      index = (int) ((current.getMillis() - start.getMillis()) / interval);
    }
    return index;
  }

  public static void main(String[] args) {
    TimeGranularity granularity = new TimeGranularity(1, TimeUnit.DAYS);

    // FAILS FOR UTC time zone
    DateTimeZone utc = DateTimeZone.forID("UTC");
    DateTime baselineStart = new DateTime(1478415600000L, utc);
    DateTime baselineEnd = new DateTime(1478678400000L, utc);

    DateTime currentStart = new DateTime(1479024000000L, utc);
    DateTime currentEnd = new DateTime(1479283200000L, utc);
    List<Range<DateTime>> currentTimeRanges = TimeRangeUtils.computeTimeRanges(granularity, currentStart, currentEnd);
    List<Range<DateTime>> baselineTimeRanges = TimeRangeUtils.computeTimeRanges(granularity, baselineStart, baselineEnd);
    System.out.println(currentTimeRanges);
    System.out.println(baselineTimeRanges);

    // WORKS FOR PST
    DateTimeZone pacificTZ = DateTimeZone.forID("America/Los_Angeles");
    baselineStart = new DateTime(1478415600000L, pacificTZ);
    baselineEnd = new DateTime(1478678400000L, pacificTZ);

    currentStart = new DateTime(1479024000000L, pacificTZ);
    currentEnd = new DateTime(1479283200000L, pacificTZ);
    currentTimeRanges = TimeRangeUtils.computeTimeRanges(granularity, currentStart, currentEnd);
    baselineTimeRanges = TimeRangeUtils.computeTimeRanges(granularity, baselineStart, baselineEnd);
    System.out.println(currentTimeRanges);
    System.out.println(baselineTimeRanges);

  }
}
