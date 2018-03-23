package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AbstractManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.pojo.EventBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * The Mock events manager for testing
 */
public class MockEventsManager extends AbstractManagerImpl<EventDTO> implements EventManager {

  private Collection<EventDTO> entities;

  /**
   * Instantiates a new Mock events manager.
   *
   * @param entities the collection of entities
   */
  MockEventsManager(Collection<EventDTO> entities) {
    super(EventDTO.class, EventBean.class);
    this.entities = entities;
  }

  @Override
  public List<EventDTO> findAll() {
    return new ArrayList<>(entities);
  }

  @Override
  public Long save(EventDTO entity) {
    entities.add(entity);
    return entity.getId();
  }

  @Override
  public int update(EventDTO entity) {
    for (EventDTO eventDTO : entities) {
      if (eventDTO.getId().equals(entity.getId())) {
        eventDTO = entity;
        return 1;
      }
    }
    return 0;
  }

  @Override
  public int delete(EventDTO entity) {
    for (EventDTO eventDTO : entities) {
      if (eventDTO.getId().equals(entity.getId())) {
        entities.remove(eventDTO);
        return 1;
      }
    }
    return 0;
  }

  @Override
  public List<EventDTO> findByEventType(String eventType) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<EventDTO> findEventsBetweenTimeRange(String eventType, long startTime, long endTime) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<EventDTO> findEventsBetweenTimeRangeByName(String eventType, String name, long startTime, long endTime) {
    throw new AssertionError("not implemented");
  }
}
