package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.anomaly.events.deployment.DeploymentEvent;
import com.linkedin.thirdeye.anomaly.events.holiday.HolidayEvent;
import java.util.List;
import org.apache.commons.lang.NullArgumentException;

public class EventDataProviderManager {
  EventDataProvider<HolidayEvent> holidayEventDataProvider;
  EventDataProvider<DeploymentEvent> deploymentEventDataProvider;

  public void registerEventDataProvider(EventType eventType,
      EventDataProvider<? extends Event> eventDataProvider) {
    switch (eventType) {
    case HOLIDAY:
      holidayEventDataProvider = (EventDataProvider<HolidayEvent>) eventDataProvider;
      break;
    case DEPLOYMENT:
      deploymentEventDataProvider = (EventDataProvider<DeploymentEvent>) eventDataProvider;
      break;
    }
  }

  public List<? extends Event> getEvents(EventFilter eventFilter) {
    if (eventFilter == null || eventFilter.getEventType() == null) {
      throw new NullArgumentException("Event filter or event type found null ");
    }
    switch (eventFilter.getEventType()) {
    case HOLIDAY:
      return holidayEventDataProvider.getEvents(eventFilter);
    case DEPLOYMENT:
      return deploymentEventDataProvider.getEvents(eventFilter);
    }
    throw new IllegalArgumentException(
        "Event type " + eventFilter.getEventType() + " not supported");
  }

}
