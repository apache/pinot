package com.linkedin.thirdeye.anomaly.events.holiday;

import com.linkedin.thirdeye.anomaly.events.EventDataProvider;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import java.util.List;

public class DefaultHolidayEventProvider implements EventDataProvider<HolidayEvent> {
  @Override
  public List<HolidayEvent> getEvents(EventFilter eventFilter) {
    return null;
  }
}
