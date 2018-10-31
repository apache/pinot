package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.util.List;

public class HolidayEventProvider implements EventDataProvider<EventDTO> {
  private EventManager eventDAO = DAORegistry.getInstance().getEventDAO();

  @Override
  public List<EventDTO> getEvents(EventFilter eventFilter) {

    List<EventDTO> allEventsBetweenTimeRange =
        eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.name(), eventFilter.getStartTime(),
            eventFilter.getEndTime());

    List<EventDTO> holidayEvents = EventFilter.applyDimensionFilter(allEventsBetweenTimeRange, eventFilter.getTargetDimensionMap());
    return holidayEvents;
  }

  @Override
  public String getEventType() {
    return EventType.HOLIDAY.toString();
  }

}
