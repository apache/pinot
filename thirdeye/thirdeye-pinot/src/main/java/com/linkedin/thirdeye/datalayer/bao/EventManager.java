package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.List;

public interface EventManager extends AbstractManager<EventDTO> {
  List<EventDTO> findByEventType(String eventType);
  List<EventDTO> findEventsBetweenTimeRange(String eventType, long startTime, long endTime);
  List<EventDTO> findEventsBetweenTimeRangeByName(String eventType, String name, long startTime, long endTime);
}
