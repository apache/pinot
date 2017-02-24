package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultHolidayEventProvider implements EventDataProvider<EventDTO> {
  private EventManager eventDAO = DAORegistry.getInstance().getEventDAO();
  private static final String COUNTRY_CODE = "countryCode";

  @Override
  public List<EventDTO> getEvents(EventFilter eventFilter) {
    List<EventDTO> allEventsBetweenTimeRange =
        eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.name(), eventFilter.getStartTime(),
            eventFilter.getEndTime());
    if (eventFilter.getTargetDimensionMap() == null
        || eventFilter.getTargetDimensionMap().size() == 0) {
      return allEventsBetweenTimeRange;
    }
    List<EventDTO> qualifiedDeploymentEvents = new ArrayList<>();
    for (Map.Entry<String, List<String>> dimensionValues : eventFilter.getTargetDimensionMap()
        .entrySet()) {
      String dimension = dimensionValues.getKey();
      List<String> targetValues = dimensionValues.getValue();
      if (dimension.startsWith("country")) {
        for (EventDTO eventDTO : allEventsBetweenTimeRange) {
          for (String target : targetValues) {
            if (eventDTO.getTargetDimensionMap().get(COUNTRY_CODE).contains(target)) {
              if (!qualifiedDeploymentEvents.contains(eventDTO)) {
                qualifiedDeploymentEvents.add(eventDTO);
              }
            }
          }
        }
      }
    }
    return qualifiedDeploymentEvents;
  }
}
