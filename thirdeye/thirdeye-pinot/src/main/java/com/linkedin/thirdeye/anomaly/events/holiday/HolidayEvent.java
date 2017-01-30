package com.linkedin.thirdeye.anomaly.events.holiday;

import com.linkedin.thirdeye.anomaly.events.ExternalEvent;

public class HolidayEvent extends ExternalEvent {
  String name;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
