package com.linkedin.thirdeye.anomaly.events.holiday;

import com.linkedin.thirdeye.anomaly.events.Event;

public class HolidayEvent extends Event {
  String name;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
