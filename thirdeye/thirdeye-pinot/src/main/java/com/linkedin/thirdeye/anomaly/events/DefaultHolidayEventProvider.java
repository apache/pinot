package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;

import java.util.List;

public class DefaultHolidayEventProvider implements EventDataProvider<EventDTO> {
  private EventManager eventDAO = DAORegistry.getInstance().getEventDAO();

  @Override
  public List<EventDTO> getEvents(EventFilter eventFilter) {

    List<EventDTO> allEventsBetweenTimeRange =
        eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.name(), eventFilter.getStartTime(),
            eventFilter.getEndTime());

    List<EventDTO> holidayEvents = EventFilter.applyDimensionFilter(allEventsBetweenTimeRange, eventFilter.getTargetDimensionMap());
    return holidayEvents;
  }

}
