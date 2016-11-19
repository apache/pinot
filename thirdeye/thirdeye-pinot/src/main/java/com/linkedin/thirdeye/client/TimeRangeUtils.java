package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

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
