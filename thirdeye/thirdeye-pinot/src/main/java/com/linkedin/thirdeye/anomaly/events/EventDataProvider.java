package com.linkedin.thirdeye.anomaly.events;

import java.util.List;

public interface EventDataProvider <T extends Event> {
  List<T> getEvents(EventFilter eventFilter);
}
