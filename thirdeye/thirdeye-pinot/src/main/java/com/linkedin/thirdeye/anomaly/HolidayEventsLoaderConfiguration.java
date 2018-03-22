package com.linkedin.thirdeye.anomaly;

import java.util.Collections;
import java.util.List;


/**
 * The type Holiday events loader configuration.
 */
public class HolidayEventsLoaderConfiguration {

  /** Specify the time range used to calculate the upper bound for an holiday's start time. In milliseconds */
  private long holidayRange;

  /** The list of calendar to fetch holidays from */
  private List<String> calendars = Collections.emptyList();

  /**
   * Gets holiday range.
   *
   * @return the holiday range
   */
  public long getHolidayRange() {
    return holidayRange;
  }

  /**
   * Sets holiday range.
   *
   * @param holidayRange the holiday range
   */
  public void setHolidayRange(long holidayRange) {
    this.holidayRange = holidayRange;
  }

  /**
   * Gets calendars.
   *
   * @return the calendars
   */
  public List<String> getCalendars() {
    return calendars;
  }

  /**
   * Sets calendars.
   *
   * @param calendars the calendars
   */
  public void setCalendars(List<String> calendars) {
    this.calendars = calendars;
  }
}
