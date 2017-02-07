package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.pojo.EventBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.List;

public class EventManagerImpl extends AbstractManagerImpl<EventDTO> implements EventManager {
  protected EventManagerImpl() {
    super(EventDTO.class, EventBean.class);
  }

  public List<EventDTO> findByEventType(String eventType) {
    Predicate predicate = Predicate.EQ("eventType", eventType);
    List<EventBean> list = genericPojoDao.get(predicate, EventBean.class);
    List<EventDTO> results = new ArrayList<>();
    for (EventBean event : list) {
      EventDTO eventDTO = MODEL_MAPPER.map(event, EventDTO.class);
      results.add(eventDTO);
    }
    return results;
  }

  public List<EventDTO> findEventsBetweenTimeRange(String eventType, long start, long end) {
    Predicate predicate = Predicate
        .AND(Predicate.EQ("eventType", eventType), Predicate.GT("endTime", start),
            Predicate.LT("startTime", end));
    List<EventBean> list = genericPojoDao.get(predicate, EventBean.class);
    List<EventDTO> results = new ArrayList<>();
    for (EventBean event : list) {
      EventDTO eventDTO = MODEL_MAPPER.map(event, EventDTO.class);
      results.add(eventDTO);
    }
    return results;
  }

  public List<EventDTO> findEventsBetweenTimeRangeByName(String eventType, String name, long start,
      long end) {
    Predicate predicate = Predicate
        .AND(Predicate.EQ("eventType", eventType), Predicate.EQ("name", name),
            Predicate.GT("endTime", start), Predicate.LT("startTime", end));
    List<EventBean> list = genericPojoDao.get(predicate, EventBean.class);
    List<EventDTO> results = new ArrayList<>();
    for (EventBean event : list) {
      EventDTO eventDTO = MODEL_MAPPER.map(event, EventDTO.class);
      results.add(eventDTO);
    }
    return results;
  }
}
