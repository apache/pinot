package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

/**
 * Not to be confused with {@link TimeRange}. This class handles splitting time windows into
 * appropriate {@link Range} objects.
 */
public class TimeRangeUtils {

  public static List<Range<DateTime>> computeTimeRanges(TimeGranularity granularity, DateTime start,
      DateTime end) {
    List<Range<DateTime>> timeranges = new ArrayList<>();
    if (granularity == null) {
      timeranges.add(Range.closedOpen(start, end));
      return timeranges;
    }
    DateTime current = start;
    while (current.isBefore(end)) {
      DateTime newCurrent = null;
      newCurrent = current.plus(granularity.toMillis());
      timeranges.add(Range.closedOpen(current, newCurrent));
      current = newCurrent;
    }
    return timeranges;
  }

  public static void main(String[] args) {
    DateTime dateTime = new DateTime();
    System.out.println(dateTime);
    System.out.println(dateTime.getMillis());
    System.out.println(DateTime.now(DateTimeZone.UTC));
    System.out.println(DateTime.now(DateTimeZone.UTC).getMillis());
  }
  
}
