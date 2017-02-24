package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.List;
import org.apache.commons.lang.NullArgumentException;

public class EventDataProviderManager {
  EventDataProvider<EventDTO> holidayEventDataProvider;
  EventDataProvider<EventDTO> deploymentEventDataProvider;

  private static final EventDataProviderManager INSTANCE = new EventDataProviderManager();

  private EventDataProviderManager() {
  }

  public static EventDataProviderManager getInstance() {
    return INSTANCE;
  }

  public void registerEventDataProvider(EventType eventType, EventDataProvider<EventDTO> eventDataProvider) {
    switch (eventType) {
    case HOLIDAY:
      holidayEventDataProvider = (EventDataProvider<EventDTO>) eventDataProvider;
      break;
    case DEPLOYMENT:
      deploymentEventDataProvider = (EventDataProvider<EventDTO>) eventDataProvider;
      break;
    }
  }

  public List<EventDTO> getEvents(EventFilter eventFilter) {
    if (eventFilter == null || eventFilter.getEventType() == null) {
      throw new NullArgumentException("EventFilter or event type found null ");
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
