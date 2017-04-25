package com.linkedin.thirdeye.anomaly.events;

import java.util.List;

public interface EventDataProvider <T> {
  List<T> getEvents(EventFilter eventFilter);

  String getEventType();
}
