package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.NullArgumentException;

public class EventDataProviderManager {

  Map<String, EventDataProvider<EventDTO>> eventDataProviderMap = new HashMap<>();

  private static final EventDataProviderManager INSTANCE = new EventDataProviderManager();

  private EventDataProviderManager() {
  }

  public static EventDataProviderManager getInstance() {
    return INSTANCE;
  }

  public void registerEventDataProvider(String eventType, EventDataProvider<EventDTO> eventDataProvider) {
    eventDataProviderMap.put(eventType, eventDataProvider);
  }

  /**
   * Fetches events from the event data provider according to the eventType in event filter
   * @param eventFilter
   * @return
   */
  public List<EventDTO> getEvents(EventFilter eventFilter) {
    if (eventFilter == null) {
      throw new NullArgumentException("EventFilter or event type found null ");
    }
    List<EventDTO> events = new ArrayList<>();
    String eventType = eventFilter.getEventType();
    if (eventType == null) {
      // return all
      for (Entry<String, EventDataProvider<EventDTO>> entry : eventDataProviderMap.entrySet()) {
        events.addAll(entry.getValue().getEvents(eventFilter));
      }
    } else {
      EventDataProvider<EventDTO> eventDataProvider = eventDataProviderMap.get(eventType);
      if (eventDataProvider != null) {
        events.addAll(eventDataProvider.getEvents(eventFilter));
      } else {
        throw new IllegalArgumentException("Event provider for event type " + eventType + " not registered");
      }
    }
    return events;
  }

}
