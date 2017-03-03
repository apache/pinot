package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.NullArgumentException;

public class EventDataProviderManager {
  EventDataProvider<EventDTO> holidayEventDataProvider;
  EventDataProvider<EventDTO> deploymentEventDataProvider;
  EventDataProvider<EventDTO> historicalAnomalyEventProvider;

  private static final EventDataProviderManager INSTANCE = new EventDataProviderManager();

  private EventDataProviderManager() {
  }

  public static EventDataProviderManager getInstance() {
    return INSTANCE;
  }

  public void registerEventDataProvider(EventType eventType, EventDataProvider<EventDTO> eventDataProvider) {
    switch (eventType) {
    case HOLIDAY:
      holidayEventDataProvider = eventDataProvider;
      break;
    case DEPLOYMENT:
      deploymentEventDataProvider = eventDataProvider;
      break;
    case HISTORICAL_ANOMALY:
      historicalAnomalyEventProvider = eventDataProvider;
      break;
    }
  }

  public List<EventDTO> getEvents(EventFilter eventFilter) {
    if (eventFilter == null) {
      throw new NullArgumentException("EventFilter or event type found null ");
    }
    if (eventFilter.getEventType() == null) {
      // This means return all events except deployments (as that can be huge)
      List<EventDTO> eventDTOList = new ArrayList<>();
      eventDTOList.addAll(holidayEventDataProvider.getEvents(eventFilter));
      eventDTOList.addAll(historicalAnomalyEventProvider.getEvents(eventFilter));
      return eventDTOList;
    }

    switch (eventFilter.getEventType()) {
    case HOLIDAY:
      return holidayEventDataProvider.getEvents(eventFilter);
    case DEPLOYMENT:
      return deploymentEventDataProvider.getEvents(eventFilter);
    case HISTORICAL_ANOMALY:
      return historicalAnomalyEventProvider.getEvents(eventFilter);
    }
    throw new IllegalArgumentException(
        "Event type " + eventFilter.getEventType() + " not supported");
  }

}
