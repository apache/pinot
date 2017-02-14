package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.ArrayList;
import java.util.List;

public class DefaultDeploymentEventProvider implements EventDataProvider<EventDTO> {

  private EventManager eventDAO = DAORegistry.getInstance().getEventDAO();

  @Override
  public List<EventDTO> getEvents(EventFilter eventFilter) {
    List<EventDTO> qualifiedDeploymentEvents = new ArrayList<>();
    List<EventDTO> allEventsBetweenTimeRange = eventDAO
        .findEventsBetweenTimeRange(EventType.DEPLOYMENT.name(), eventFilter.getStartTime(),
            eventFilter.getEndTime());

    // TODO: write go/informed query app to fetch deployment events onthe fly.

    // TODO: filter qualified events from allEvents based on filter criteria
    return qualifiedDeploymentEvents;
  }
}
