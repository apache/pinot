package com.linkedin.thirdeye.anomaly;

import java.util.Collections;
import java.util.List;


/**
 * The type Holiday events loader configuration.
 */
public class HolidayEventsLoaderConfiguration {

  /** Specify the time range used to calculate the upper bound for an holiday's start time. In milliseconds */
  private long holidayLoadRange;

  /** The list of calendar to fetch holidays from */
  private List<String> calendars = Collections.emptyList();

  /** Run frequency of holiday events loader (Days) */
  private int runFrequency;

  /**
   * Gets run frequency.
   *
   * @return the run frequency
   */
  public int getRunFrequency() {
    return runFrequency;
  }

  /**
   * Sets run frequency.
   *
   * @param runFrequency the run frequency
   */
  public void setRunFrequency(int runFrequency) {
    this.runFrequency = runFrequency;
  }

  /**
   * Gets holiday load range.
   *
   * @return the holiday load range
   */
  public long getHolidayLoadRange() {
    return holidayLoadRange;
  }

  /**
   * Sets holiday load range.
   *
   * @param holidayLoadRange the holiday load range
   */
  public void setHolidayLoadRange(long holidayLoadRange) {
    this.holidayLoadRange = holidayLoadRange;
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
